# -*- coding: latin-1 -*-
#
# Copyright 2021-2021 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-administration
#
# All rights reserved.
#
"""
This file contains the tools for automated syncing administration

@author: Kenneth Waegeman (Ghent University)
"""
import logging

from vsc.accountpage.sync import Sync
from vsc.administration.user import update_user_status
from vsc.administration.tier1c import VscTier1cUser, VscTier1cProject
from vsc.utils import fancylogger

log = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()

STORAGE_USERS_LIMIT_WARNING = 1
STORAGE_USERS_LIMIT_CRITICAL = 10
STORAGE_QUOTA_LIMIT_WARNING = 1
STORAGE_QUOTA_LIMIT_CRITICAL = 5
STORAGE_PRJ_LIMIT_WARNING = 1
STORAGE_PRJ_LIMIT_CRITICAL = 2

#TODO: vsc-config tier1c names
VSC_TIER1C_SCRATCH = 'dodrioscratch'


class VscTier1cSync(Sync):
    """
    Sync the tier1c users, projects and quota
    """

    def process_t1_users(self, account_ids, quotas, storage_name, dry_run=False):
        """
        Process the users.
        If all users are part of a project, and only have actual data/directories inside projects
        this method should be integraded with process_t1_projects

        """
        error_users = []
        ok_users = []

        if storage_name != VSC_TIER1C_SCRATCH:
            log.raiseException("%s is not a storage name for tier1c" % storage_name)

        for vsc_id in sorted(account_ids):
            user = VscTier1cUser(vsc_id, rest_client=self.apc, use_user_cache=True, dry_run=dry_run)

            try:
                user.create_home_dir()
                user.populate_home_dir()
                update_user_status(user, self.apc)

                ok_users.append(user)
            except Exception:
                log.exception("Cannot process user %s", user.user_id)
                error_users.append(user)

        ok_quota = []
        error_quota = []

        for quota in quotas:
            user = VscTier1cUser(quota.user, rest_client=self.apc, use_user_cache=True, dry_run=dry_run)

            try:
                user.set_quota()
                ok_quota.append(quota)
            except Exception:
                log.exception("Cannot process user %s", user.user_id)
                error_quota.append(quota)

        return (ok_users, error_users, ok_quota, error_quota)

    def process_projects(self, prj_ids, storage_name, datestamp, dry_run=False):
        """Process the projects.

        - make the fileset per project
        - set the quota for the project
        - set the quota on a per-user basis for all project members
        """

        listm = Monoid([], lambda xs, ys: xs + ys)
        ok_prj = MonoidDict(copy.deepcopy(listm))
        error_prj = MonoidDict(copy.deepcopy(listm))

        if storage_name != VSC_TIER1C_SCRATCH:
            log.raiseException("%s is not a storage name for tier1c" % storage_name)

        for prj_id in sorted(prj_ids):

            prj = VscTier1cProject(prj_id, rest_client=self.apc, dry_run=dry_run)

            try:
                prj.create_scratch_fileset()
                prj.set_scratch_quota()

                modified_member_list = prj.modified_members(datestamp)
                factory = lambda pid: VscTier1cUser(pid,
                                                    rest_client=self.apc,
                                                    use_user_cache=True,
                                                    dry_run=dry_run)
                modified_members = [factory(a["vsc_id"]) for a in modified_member_list]

                for member in modified_members:
                    try:
                        prj.set_member_scratch_quota(member)  # half of the project quota
                        prj.create_member_scratch_dir(member)

                        ok_prj[prj.prj_id] = [member.account.vsc_id]
                    except Exception:
                        log.exception("Failure at setting up the member %s of project %s on %s",
                                      member.account.vsc_id, prj.prj_id, storage_name)
                        error_prj[prj.prj_id] = [member.account.vsc_id]
            except Exception:
                log.exception("Something went wrong setting up the project %s on the storage %s",
                    prj.prj_id, storage_name)
                error_prj[prj.prj_id] = prj.members

        return (ok_prj, error_prj)

    def do(self, dry_run):
        """
        Actual work
        - build the filter
        - fetches the users/projects
        - process the users/projects
        - write the new timestamp if everything went OK
        - write the nagios check file
        """

        stats = {}
        #FIXME: decide these below and move to vsc-config stuff
        institute = 'GhentTier1c'
        storage_name = VSC_TIER1C_SCRATCH
        project_prefix = 'proj_'

        (users_ok, users_fail) = ([], [])
        (quota_ok, quota_fail) = ([], [])
        if self.options.user:
            #FIXME: How exactly to fetch tier1c users from AP? see as seperate site or parse attribute
            changed_accounts, _ = self.get_accounts(site=institute)
            #FIXME: parse accounts for tier1?
            log.info("Found %d %s accounts that have changed in the accountpage since %s" %
                        (len(changed_accounts), institute, self.start_timestamp))

            storage_changed_quota = self.get_user_storage_quota(storage_name=storage_name)
            log.info("Found %d quota changes on storage %s in the accountpage",
                len(storage_changed_quota), storage_name)

            (users_ok, users_fail, quota_ok, quota_fail) = self.process_t1_users(
                changed_accounts,
                storage_changed_quota,
                storage_name,
                dry_run=dry_run)
            stats["%s_users_sync" % (storage_name,)] = len(users_ok)
            stats["%s_users_sync_fail" % (storage_name,)] = len(users_fail)
            stats["%s_users_sync_fail_warning" % (storage_name,)] = STORAGE_USERS_LIMIT_WARNING
            stats["%s_users_sync_fail_critical" % (storage_name,)] = STORAGE_USERS_LIMIT_CRITICAL
            stats["%s_quota_sync" % (storage_name,)] = len(quota_ok)
            stats["%s_quota_sync_fail" % (storage_name,)] = len(quota_fail)
            stats["%s_quota_sync_fail_warning" % (storage_name,)] = STORAGE_QUOTA_LIMIT_WARNING
            stats["%s_quota_sync_fail_critical" % (storage_name,)] = STORAGE_QUOTA_LIMIT_CRITICAL

        (prj_ok, prj_fail) = ([], [])
        if self.options.projects:
            #FIXME: How exactly to fetch tier1c projects from AP? see as seperate site or parse attribute
            changed_groups, _ = self.get_groups(site=institute)
            changed_prjs = [g for g in changed_groups if g.vsc_id.startswith(project_prefix)]
            #FIXME: can we use vo logic of accountpage below for projects?
            changed_prj_quota = [q for q in self.get_vo_storage_quota(storage_name=storage_name)
                if q.fileset.startswith(project_prefix)]

            #FIXME: virtual_organisation -> project in AP?
            prjs = sorted(set([v.vsc_id for v in changed_prjs] + [v.virtual_organisation for v in changed_prj_quota]))

            log.info("Found %d %s projects that have changed in the accountpage since %s" %
                        (len(changed_prjs), institute, self.start_timestamp))
            log.info("Found %d %s VOs that have changed quota in the accountpage since %s" %
                        (len(changed_prj_quota), institute, self.start_timestamp))
            log.debug("Found the following {institute} projects: {prjs}".format(institute=institute, prjs=prjs))

            (prj_ok, prj_fail) = self.process_projects(
                prjs,
                storage_name,
                self.start_timestamp,
                dry_run=dry_run)
            stats["%s_prj_sync" % (storage_name,)] = len(prj_ok)
            stats["%s_prj_sync_fail" % (storage_name,)] = len(prj_fail)
            stats["%s_prj_sync_fail_warning" % (storage_name,)] = STORAGE_PRJ_LIMIT_WARNING
            stats["%s_prj_sync_fail_critical" % (storage_name,)] = STORAGE_PRJ_LIMIT_CRITICAL

        self.thresholds = stats

        if users_fail or quota_fail or prj_fail:
            return users_fail + quota_fail + prj_fail
        else:
            return False

