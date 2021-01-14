#!/usr/bin/env python
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
This script synchronises the users and projects from the HPC account page to the Tier1c scratch storage.

For each (active) user/project, the following tasks are done:
    - create directories in the scratch filesystem
    - chown this directory to the user/group
    - create the basic directories and scripts if they do not yet exist (.ssh, .bashrc, ...)
    - drop the user's public keys in the appropriate location
    - chmod the files to the correct value
    - chown the files (only changes things upon first invocation and new files)

The script should result in an idempotent execution, to ensure nothing breaks.
"""
from vsc.administration.sync import VscTier1cSync
import logging

from vsc.utils import fancylogger

NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes

logger = fancylogger.getLogger()
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()


VscTier1cSync.CLI_OPTIONS = {
    'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
    'user': ('process users', None, 'store_true', False),
    'projects': ('process projects', None, 'store_true', False),
}

if __name__ == '__main__':
    VscTier1cSync().main()

