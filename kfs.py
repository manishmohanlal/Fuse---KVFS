'''
Created on May 3, 2012

@author: anduril
'''
import fuse
import os
import stat
import errno
from kfsentity import KFSEntity
import logging

fuse.fuse_python_api = (0, 2)
logging.basicConfig(filename='kfs.log',level=logging.DEBUG)
print "test"

def zstat(stat):
    stat.st_mode  = 0
    stat.st_ino   = 0
    stat.st_dev   = 0
    stat.st_nlink = 2
    stat.st_uid   = 0
    stat.st_gid   = 0
    stat.st_size  = 0
    stat.st_atime = 0
    stat.st_mtime = 0
    stat.st_ctime = 0
    return stat

class KFS(fuse.Fuse):
    def __init__(self, *args, **kwargs):
        fuse.Fuse.__init__(self, *args, **kwargs)
        
        self.uid = os.getuid()
        self.gid = os.getgid()
        
        root_dir = KFSEntity(0755 | stat.S_IFDIR, self.uid, self.gid)
        self._storage = {'/' : root_dir}
    
    def getattr(self, path):
        if not path in self._storage:
            return -errno.ENOENT
        
        item = self._storage[path]
        st = zstat(fuse.Stat())
        st.st_mode  = item.mode
        st.st_uid   = item.uid
        st.st_gid   = item.gid
        st.st_atime = item.atime
        st.st_mtime = item.mtime
        st.st_ctime = item.ctime
        st.st_size  = len(item.data)
        logging.info('returning path entity - '+str(st))
        return st
    
    def mkdir(self, path, mode):
        self._storage[path] = KFSEntity(mode | stat.S_IFDIR, self.uid, self.gid)
        logging.info("created entry in key-value store for new dir.")
        logging.info("updating parent. started..")
        self._add_to_parent_dir(path)
        
    def readdir(self, path, offset):
        dir_items = self._storage[path].data
        for item in dir_items:
            yield fuse.Direntry(item)
            
    def _add_to_parent_dir(self, path):
        parent_path = os.path.dirname(path)
        filename = os.path.basename(path)
        self._storage[parent_path].data.add(filename)
        logging.info("updated parent content - completed.")
            
        

def main():
    usage="""
        KVFS - Key Value File System
    """ + fuse.Fuse.fusage
    logging.info("creating a new instance of filesystem")
    server = KFS(version="%prog "+ fuse.__version__, 
                    usage=usage, dash_s_do='setsingle')
    server.parse(errex=1)
    logging.info("Calling main for file system")
    server.main()
    

if __name__ == '__main__':
    print "test"
    main()