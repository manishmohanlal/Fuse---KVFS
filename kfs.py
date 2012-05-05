import os, sys, stat, errno, time
import fuse
from metafs import MetaFS
fuse.fuse_python_api = (0, 2)

import logging
LOG_FILENAME = 'log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)

class KFS(fuse.Fuse):
    def __init__(self, metafs, *args, **kwargs):
	fuse.Fuse.__init__(self, *args, **kwargs)
	self.metafs=metafs

    # --- Metadata -----------------------------------------------------------
    def getattr(self, path):
	logging.info("getattr")
	return self.metafs.getattr(path)

    def chmod(self, path, mode):
	return self.metafs.chmod(path, mode)

    def chown(self, path, uid, gid):
	return self.metafs.chown(path, uid, gid)

    def utime(self, path, times):
	return self.metafs.utime(path, times)

    # --- Namespace ----------------------------------------------------------
    def unlink(self, path):
	return self.metafs.unlink(path)

    def rename(self, oldpath, newpath):
	return self.metafs.rename(self, oldpath, newpath)

    # --- Links --------------------------------------------------------------
    def symlink(self, path, newpath):
	return self.metafs.symlink(path, newpath)

    def readlink(self, path):
	return self.metafs.readlink(path)


    # --- Files --------------------------------------------------------------
    def mknod(self, path, mode, dev):
	return self.metafs.mknod(path, mode, dev)

    def create(self, path, flags, mode):
	return self.metafs.create(path, flags, mode)

    def truncate(self, path, len):
	return self.metafs.truncate(path, len)

    def read(self, path, size, offset):
	return self.metafs.read(path, size, offset)

    def write(self, path, buf, offset):
	return self.metafs.write(path, buf, offset)

    # --- Directories --------------------------------------------------------
    def mkdir(self, path, mode):
	logging.info("Inside mkdir")
	return self.metafs.mkdir(path, mode)

    def rmdir(self, path):
	return self.metafs.rmdir(path)

    def readdir(self, path, offset):
	return self.metafs.readdir(path, offset)


def main():
    usage="""
HTFS - HashTable File-System

""" + fuse.Fuse.fusage
    metafs = MetaFS()
    server = KFS(metafs,version="%prog " + fuse.__version__,usage=usage,dash_s_do='setsingle')

    server.parse(errex=1)
    server.main()

if __name__ == '__main__':
    main()


