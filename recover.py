#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
#  This program is used to revert lazylibrarian back to a known working state
#  if an update fails. It unpacks "backup.tgz" to replace the update

import tarfile

archivename = 'backup.tgz'

if tarfile.is_tarfile(archivename):
    try:
        with tarfile.open(archivename) as tar:
            tar.extractall()
    except Exception as e:
        print('error', 'Failed to unpack tarfile %s (%s): %s' %
              (type(e).__name__, archivename, str(e)))
else:
    print("Invalid archive")
exit(0)
