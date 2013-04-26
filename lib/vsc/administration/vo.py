#!/usr/bin/env python
# -*- coding: latin-1 -*-
##
# Copyright 2012-2013 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# All rights reserved.
#
##
#!/usr/bin/env python
##
"""
This file contains the utilities for dealing with VOs on the VSC.
Original Perl code by Stijn De Weirdt

@author: Stijn De Weirdt (Ghent University)
@author: Andy Georges (Ghent University)
"""

import os
import re

from lockfile.pidlockfile import PIDLockFile

from vsc import fancylogger
from vsc.administration.institute import Institute
from vsc.administration.user import VscUser
from vsc.config.base import VSC, Muk, CentralStorage
from vsc.filesystem.gpfs import GpfsOperations
from vsc.ldap.entities import VscLdapGroup

logger = fancylogger.getLogger(__name__)

VO_PREFIX = 'gvo'
DEFAULT_VO = 'gvo000012'
INSTITUTE_VOS = ['gvo00012', 'gvo00016', 'gvo00017', 'gvo00018']


class VscVo(VscLdapGroup):
    """Class representing a VO in the VSC.

    A VO is a special kind of group, identified mainly by its name.
    """

    def __init__(self, vo_id, storage=None):
        """Initialise"""
        super(VscVo, self).__init__(vo_id)

        # Normally, we could use the group_id, but since we're in VO, we should use the right terms
        self.vo_id = vo_id
        self.vsc = VSC()

        if not storage:
            self.storage = CentralStorage()

        self.gpfs = GpfsOperations()

    def _lock(self):
        """Take a global lock (on a file), to avoid other instances
        ruining things :-)
        """
        pass

    def members(self):
        """Return a list with all the VO members in it."""
        return [VscUser(m) for m in self.memberUid]

    def _data_path(self):
        """Return the path to the VO data fileset on GPFS"""
        data = self.gpfs.get_filesystem_info(self.storage.data_name)
        return os.path.join(data['defaultMountPoint'], 'vos', self.vo_id[:-2], self.vo_id)

    def _scratch_path(self):
        """Return the path to the VO data fileset on GPFS"""
        scratch = self.gpfs.get_filesystem_info(self.storage.scratch_name)
        return os.path.join(scratch['defaultMountPoint'], 'vos', self.vo_id[:-2], self.vo_id)

    def _create_fileset(self, filesystem_name, path, quota=True):
        """Create a fileset for the VO on the data filesystem.

        - creates the fileset if it does not already exist
        - sets the (fixed) quota on this fileset for the VO
        """
        self.gpfs.list_filesets()
        fileset_name = self.vo_id

        if not self.gpfs.get_fileset_info(filesystem_name, fileset_name):
            self.log.info("Creating new fileset on %s scratch with name %s and path %s" % (filesystem_name, fileset_name, path))
            base_dir_hierarchy = os.path.dirname(path)
            self.gpfs.make_dir(base_dir_hierarchy)
            self.gpfs.make_fileset(path, fileset_name)
        else:
            self.log.info("Fileset %s already exists for VO %s ... not creating again." % (fileset_name, self.vo_id))

        self.gpfs.chmod(0700, path)
        self.gpfs.chown(int(self.gidNumber), int(self.gidNumber), path)

    def create_data_fileset(self):
        """Create the VO's directory on the HPC data filesystem. Always set the quota."""
        try:
            path = self._data_path()
            self._create_fileset(self.storage.data_name, path, self.dataQuota)
        except AttributeError, err:
            self.log.exception("No data_name attribute in the storage instance %s" % (self.storage))

    def create_scratch_fileset(self):
        """Create the VO's directory on the HPC data filesystem. Always set the quota."""
        try:
            path = self._scratch_path()
            self._create_fileset(self.storage.scratch_name, path, self.dataQuota)
        except AttributeError, err:
            self.log.exception("No scratch_name attribute in the storage instance %s" % (self.storage))

    def _create_vo_dir(self, path, quota=None):
        """Create a user owned directory on the GPFS."""
        self.gpfs.make_dir(path)


    def _set_quota(self, path_function, quota):
        """Set FILESET quota on the FS for the VO fileset.

        @type quota: int

        @param quota: soft quota limit expressed in KiB
        """
        try:
            path = path_function()
            quota *= 1024
            soft = int(quota * self.vsc.quota_soft_fraction)

            # LDAP information is expressed in KiB, GPFS wants bytes.
            self.gpfs.set_fileset_quota(soft, path, self.vo_id, quota)
            self.gpfs.set_fileset_grace(path, self.vsc.vo_storage_grace_time)  # 7 days
        except AttributeError, err:
            self.log.exception("No such attribute in the storage instance %s" % (self.storage))

    def set_data_quota(self):
        """Set FILESET quota on the data FS for the VO fileset."""
        self._set_quota(self._data_path, int(self.dataQuota))

    def set_scratch_quota(self):
        """Set FILESET quota on the scratch FS for the VO fileset."""
        self._set_quota(self._scratch_path, int(self.scratchQuota))

    def _set_member_quota(self, path_function, member, quota):
        """Set USER quota on the FS for the VO fileset

        @type member: VscUser instance
        """
        try:
            path = path_function()
            soft = int(quota * self.vsc.quota_soft_fraction)
            self.gpfs.set_user_quota(soft, int(member.uidNumber), path, quota)
        except AttributeError, err:
            self.log.exception("No such attribute in the storage instance %s" % (self.storage))

    def set_member_data_quota(self, member):
        """Set the quota on the data FS for the member in the VO fileset.

        @type member: VscUser instance

        The user can have up to half of the VO quota.
        FIXME: This should probably be some variable in a config setting instance
        """
        quota = int(self.dataQuota) / 2 * 1024  # expressed in bytes
        self._set_member_quota(self._data_path, member, quota)

    def set_member_scratch_quota(self, member):
        """Set the quota on the scratch FS for the member in the VO fileset.

        @type member: VscUser instance

        The user can have up to half of the VO quota.
        FIXME: This should probably be some variable in a config setting instance
        """
        quota = int(self.dataQuota) / 2 * 1024
        self._set_member_quota(self._scratch_path, member, quota)


class VoOld(object):
    """Class representing a VO in the VSC administrative library.

    A VO is a special kind of group.

    #FIXME: needs rewrite
    """
    def __init__(self, institute_name):
        super(VoOld, self).__init__(institute_name)
        self.logger = fancylogger.getLogger(self.__class__.__name__)
        self.USER_LOCKFILE_NAME = "/tmp/lock.%s.pid" % (self.__class__.__name__)
        self.lockfile = PIDLockFile(self.USER_LOCKFILE_NAME)

        self.description = None
        self.fairshare = None
        self.members = None

    def __setup(self, ldap_attributes):
        """Fill in the instance values from a retrieved VO.

        @type ldap_attributes: dictionary with the required VO attributes as keys.

        Sets self.exists to True if the data is filled in.
        """
        if ldap_attributes is not None:
            self.cn = ldap_attributes['cn'],
            self.description = ldap_attributes['description'],
            self.gid_number = ldap_attributes['gidNumber'],
            self.institute = Institute(ldap_attributes['institute']),
            self.fairshare = ldap_attributes['fairshare'],
            self.moderators = ldap_attributes['moderator'],
            self.members = ldap_attributes.get('memberUid', [])
            self.exists = True
        else:
            self.exists = False

    def load(self, vo_name):
        """Load the VO data from the LDAP.

        @type vo_name: string representing a VO in the VSC.

        @raise NoSuchInstituteError, NoSuchVoError
        """
        vo = self.ldap_query.vo_filter_search("(&(cn=%s) (institute=%s))" % (vo_name, self.institute.institute_name))
        if not vo:
            self.exists = False
            raise NoSuchVoError(vo_name)
        self.__setup(vo[0])
        return self

    def __generate_name(self):
        """Generate a name for a new VO.

        - The generated name depends on the institute.
        - This function is not protected by a lock, so make sure you protect in
          upwards in the call stack.

        @returns: a string representing the name of a new VO.
        """
        vos = self.ldap_query.vo_filter_search("(institute=%s)" % (self.institute.institute_name))
        if vos is None:
            highest = 0
        else:
            vo_number_regex = re.compile('.vo(?P<number>\d+)')
            vo_numbers = [vo_number_regex.search(v).groups('number')[0] for v in vos]
            highest = int(max(vo_numbers))

        highest += 1
        name = self.institute.institute_name[0] + 'vo' + '0' * (5-len(str(highest))) + str(highest)

        logger.info("Generated new VO name: %s" % (name))
        return name

    def add(self, moderator_uid, description):
        """Add a VO to the VSC LDAP database.

        Note that the VO name is automagically generated

        @type moderator_uid: string representing the user ID on the VSC of a VO moderator
        @type description: description of the VO

        @raise: NoSuchInstitute
        """
        vo_group_id = self.get_next_group_id(self.institute.institute_name)
        vo_name = self.__generate_name()
        fairshare = "100"
        attributes = {
            'objectclass': [ 'top', 'posixGroup', 'vscgroup' ],
            'cn': vo_name,
            'description': description,
            'gidNumber': str(vo_group_id),
            'institute': self.institute.institute_name,
            'fairshare': fairshare,
            'moderator': moderator_uid,
            'status': self.ldap_query.ldap.vsc.defaults['new_user_status']
        }
        self.ldap_query.group_add(vo_name, attributes)
        self.__setup(attributes)

    def add_member(self, member_uid):
        """Add a member to a VO.

        @type member_uid: the user id of the user on the VSC

        @returns: The user_id if the user was added. None otherwise.
        """
        if not self.exists:
            self.logger.error("%s.add_member: trying to add %s to non-initialised group" % (self.__class__.__name__, member_uid))
            return None

        # first remove the member from the default VO unless we're the default VO
        (default_vo_name, _) = self.institute.get_default_vo_admin_info()
        if self.cn != default_vo_name:
            default_vo = Vo(self.institute).load(default_vo_name)
            default_vo.delete_member(member_uid)

        super(Vo, self).add_member(member_uid)

    def delete_member(self, member_uid):
        """Remove a user from a VO.

        @type member_uid: string representing the id of a VSC member
        """
        if not self.exists:
            self.logger.error("%s.delete_member: trying to delete %s from non-initialised group" % (self.__class__.__name__, member_uid))
            return None

        super(Vo, self).delete_member(member_uid)

        # move the member back to the default VO.
        (default_vo_name, _) = self.institute.get_default_vo_admin_info()
        if self.cn != default_vo_name:
            default_vo = Vo(self.institute).load(default_vo_name)
            default_vo.add_member(member_uid)


    def add_moderator(self, member_uid):
        """Add the user with given ID to the moderator set for the given VO.

        Note that checking if the user actually exists (in the institute the
        VO belongs to!) is the responsibility of the calling code.

        @type member_uid: string representing the user id on the VSC

        @returns: the member_uid if the member has been added as a moderator, None otherwise.
        """
        if not self.exists:
            return None

        # there certainly is at least one VO the user belongs to (e.g., the default VO),
        # and there should be no more than one
        (default_vo_name, _) = self.institute.get_default_vo_admin_info()
        vos = self.ldap_query.vo_filter_search("(|(memberUid=%s) (moderator=%s))" % (member_uid, member_uid), attributes=['cn'])
        for v in vos:
            if v['cn'] == self.cn or v['cn'] == default_vo_name:
                return None

        # if the member is not yet a member of this VO, add him
        # this will also remove the member from the default VO
        self.add_member(member_uid)
        if not member_uid in self.moderators:
            moderators = self.moderators + [member_uid]
            self.ldap_query.group_modify(self.cn, { 'moderator' : moderators})
            self.moderators = moderators
        return self

    def delete_moderator(self, member_uid):
        """Remove the given user as a moderator for this VO.

        @type member_uid: string representing the member on the VSC

        @raise: UserDoesNotExist
        """
        if not self.exists:
            self.logger.error("%s.delete_moderator: trying to delete %s from non-initialised vo" % (self.__class__.__name__, member_uid))
            return None

        if not member_uid in self.moderators:
            raise NoSuchVoModeratorError(self.cn, member_uid)

        moderators = self.moderators
        moderators.remove(member_uid)
        ## safety check!
        if not moderators:
            return None
        else:
            self.ldap_query.group_modify(self.cn, { 'moderator': moderators })
            self.moderators = moderators
            return self

    def modify_quota(self, data_quota=None, scratch_quota=None):
        """Change the quota for the given VO.

        If there have been no quota set for the VO, they are
        added to the LDAP entry.

        @type data_quota: integer indicating the quota on the data filesystem
        @type scratch_quota: integer indicating the quota on the scratch filesystem
        """
        if not self.exists:
            self.logger.error("%s.modify_quota: trying to change quota (%d, %d) to non-initialised vo" % (self.__class__.__name__, data_quota, scratch_quota))
            return None

        self.logger.info("Changing quota for vo %s to %d [data] and %d [scratch]" % (self.cn, data_quota, scratch_quota))

        if data_quota is not None:
            self.__modify_quota('dataDirectory', self.ldap_query.ldap.vsc.vo_pathnames()['data'], 'dataQuota', data_quota)

        if scratch_quota is not None:
            self.__modify_quota('scratchDirectory', paths['scratch'], 'scratchQuota', scratch_quota)

    def __modify_quota(self, path_name, path, quota_name, quota):
        """Change the quota settings in the LDAP entry for the given VO.

        If there have been no quota set, add the required entries.

        @type path_name: string representing the name of the entry in both the LDAP and the VO class instance
        @type path: string representing the location on which the quota should be placed
        @type quota_name: string representing the name of the entry in both the LDAP and the VO class instance
        @type quota: integer representing the quota
        """
        self.ldap_query.group_modify(self.cn, { path_name: path })
        self.__dict__[path_name] = path
        self.ldap_query.group_modify(self.cn, { quota_name: quota})
        self.__dict__[quota_name] = quota


