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
This file contains the tools for tier1c administration

@author: Kenneth Waegeman (Ghent University)
"""
import os
import pwd
from vsc.administration.user import VscAccountPageUser
from vsc.administration.vo import VscAccountPageVo
from vsc.utils import fancylogger
from vsc.config.base import GENT, VSC, VscStorage, Storage
from vsc.accountpage.wrappers import mkVscAccount

from vsc.filesystem.lustre import LustreOperations
from vsc.filesystem.gpfs import GpfsOperations
log = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()

PROJECT_SUBDIR = 'projects'
PROJ = 'prj'

class Dodrio(Storage):
    """Temporary, for testing eyes only/ Migrate me to etc/filesystem_info templates"""
    # FIXME: migrate
    def __init__(self):
        super().__init__(
                filesystem='dodrioscratch',
                # Two new fields without hard dependency on gpfs, FIXME others still need values
                fsbackend='lustre',
                mount_point='/dodrio/scratch',
                #metadata_replication_factor=2
                #quota_user=1048576
                #quota_user_inode=20000
                quota_prj=1048576,
                #quota_vo_inode=1048576
                #user_grouping_fileset=1
                #user_path=/arcanine/scratch
                )

class VscStorageBackend(object):
    """
    A backend Vsc Storage system
    """
    def __init__(self, storage=None, path_templates=None, storage_name=None, host_institute=GENT, dry_run=False):

        if storage is None or path_templates is None:
            if storage_name is None or host_institute is None:
                log.raiseException('Can not find storage backend without storage or storage_name/institute defined')
            else:
                vscstorage = VscStorage()
                storage = vscstorage[host_institute][storage_name]
                path_templates = vscstorage.path_templates[host_institute][storage_name]

        self.storage = storage
        self.storage_name = storage.filesystem
        self.path_templates = path_templates

        self.vsc = VSC()

        if self.storage.fsbackend == 'lustre':
            self.fsops = LustreOperations()
        elif self.storage.fsbackend == 'gpfs':
            self.fsops = GpfsOperations()
        else:
            log.raiseException('FS backend not yet supported')

    def _get_path(self, template_type, name):
        (path, _) = self.path_templates[template_type](name)
        return os.path.join(self.storage.mount_point, path)

    def _get_project_path(self, pjname):
        return self._get_path(PROJ, pjname)

    def _create_fileset(self, path, name, mode=0o770):
        """ create a fileset on storage """
        if not self.fsops.get_fileset_info(self.storage_name, name):
            log.info("Creating new fileset on %s with name %s and path %s",
                self.storage_name, name, path)
            base_dir_hierarchy = os.path.dirname(path)
            self.fsops.make_dir(base_dir_hierarchy)
            self.fsops.make_fileset(path, name)
        else:
            log.info("Fileset %s already exists for %s ... not creating again.", name, self.storage_name)

        self.fsops.chmod(mode, path)

    def create_project_fileset(self, pjname, moderator_id, group_owner_id):
        """ create a fileset for a project on storage """
        path = self._get_project_path(pjname)
        self._create_fileset(path, pjname)
        self.fsops.chown(moderator_id, group_owner_id, path)

    def _set_fileset_quota(self, path, name, quota):
        """ Set quota on a fileset on storage """
        try:
            # expressed in bytes, retrieved in KiB from the backend
            hard = quota * 1024 * self.storage.data_replication_factor
            soft = int(hard * self.vsc.quota_soft_fraction)

            self.fsops.set_fileset_quota(soft, path, name, hard)
            self.fsops.set_fileset_grace(path, self.vsc.vo_storage_grace_time)

        except Exception:
            log.raiseException("Unable to set quota on path %s", path)


    def set_project_quota(self, pjname, quota=None):
        """ set project quota on storage """
        path = self._get_project_path(pjname)
        if not quota:
            quota = self.storage.quota_prj
            log.error("No %s quota information available for %s", pjname, self.storage_name)
            log.info("Setting default quota for %s on storage %s to %d",
                         pjname, self.storage_name, quota)
        else:
            quota = quota.hard

        log.info("Setting %s quota on storage %s to %d", pjname, self.storage_name, quota)
        self._set_fileset_quota(path, pjname, quota)

    def _create_member_dir(self, target, uid, gid, mode=0o700, override_permissions=False):
        """Create a member-owned directory in the fileset."""
        self.fsops.create_stat_directory(
            target,
            mode,
            int(uid),
            int(gid),
            override_permissions)

    def create_project_member_dir(self, pjname, member, muid, mgid):
        target = os.path.join(self._get_project_path(pjname), member)
        self._create_member_dir(target, muid, mgid)


class VscTier1cUser(VscAccountPageUser):
    '''VSC  Tier1c AccountPage User with Lustre Backend '''
    def __init__(self, user_id, storage_name=None, storage=None, rest_client=None, account=None,
            dry_run=False, use_user_cache=False):
        """
        Initialisation.
        @type vsc_user_id: string representing the user's VSC ID (vsc[0-9]{5})
        """

        if storage is None:
            storage = Dodrio()

        self.backend = VscStorageBackend(storage=storage, dry_run=dry_run)

        super().__init__(user_id, rest_client=rest_client, account=account, use_user_cache=use_user_cache)

    def set_quota(self):
        """ Setting global quota would limit quota in projects to this..
            we could create 'projects' for users but that's ugly """
        return

    #FIXME: implement more functions(homeonscratch,..) ?

# FIXME: reuse VO logic or use a VscAccountPageProject class/accountpage implementation
class VscTier1cProject(VscAccountPageVo):
    '''VSC  Tier1c AccountPage Project within a Lustre file system '''
    def __init__(self, pjid, storage_name=None, storage=None, rest_client=None, host_institute=GENT, dry_run=False):

        if storage is None: #FIXME: Move to vsc-config
            storage = Dodrio()
            path_templates = { PROJ : lambda pjname: self.project_path(pjname, host_institute) }

        if storage_name is None:
            storage_name = storage.filesystem

        self.storage_name = storage_name

        self.backend = VscStorageBackend(storage=storage, storage_name=storage_name,
                        path_templates=path_templates, dry_run=dry_run)

        super().__init__(pjid, rest_client=None)
        self.pjid = self.vo_id

    @property
    def prj_quota(self):
        """ Get the prj quota from AP """
        if not self._quota_cache:
            # FIXME: get from AP
            self._quota_cache = None

        return self._quota_cache

    @property
    def prj(self):
        """ prj in AP which currently uses vo logic """
        #FIXME: how to handle projects from AP
        return self.vo

    def project_path(self, pjid, host_institute):
        """ Path templates for tier1c. Can be integraded with vsc-config """
        return (os.path.join(host_institute, PROJECT_SUBDIR, pjid), None)



    def create_fileset(self):
        """ Create the project scratch fileset """

        try:
            moderator = mkVscAccount(self.rest_client.account[self.prj.moderators[0]].get()[1])
            fileset_group_owner_id = self.prj.vsc_id_number
            fileset_owner_id = moderator.vsc_id_number
        except HTTPError:
            log.exception("Cannot obtain moderator information from account page, setting ownership to nobody")
            fileset_owner_id = pwd.getpwnam('nobody').pw_uid
        except IndexError:
            log.error("There is no moderator available for %s", self.prj.vsc_id)
            fileset_owner_id = pwd.getpwnam('nobody').pw_uid
        try:
            self.backend.create_project_fileset(self.pjid, fileset_owner_id, fileset_group_owner_id)

        except Exception:
            log.raiseException("Failed to create fileset %s ", self.pjid)


    def set_quota(self):
        """ Set the project scratch quota """
        quota = self.prj_quota
        self.backend.set_project_quota(self.pjid, quota)



    def modified_members(self, datestamp):
        """ Get a list of modified members of this project """
        return self.prj.member.modified[datestamp].get()

    def create_member_dir(self, member):
        """Create a directory in the fileset that is owned
        by the member with name <PROJECT>/<vscid>."""
        self.backend.create_project_member_dir(self, self.pjid, member.user_id,
                member.account.vsc_id_number, member.usergroup.vsc_id_number)


    def set_member_scratch_quota(self, member):
        """ Note: we can't do this (yet) on Lustre, we only can set global user quota """
        return
