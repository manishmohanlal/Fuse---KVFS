'''
Created on May 4, 2012

@author: anduril
'''
import time
import stat

class KFSEntity(object):
    def __init__(self, mode, uid, gid ):
        self.atime = time.time()
        self.ctime = self.atime
        self.mtime = self.atime
        
        self.mode = mode
        self.uid = uid
        self.gid = gid
        
        if stat.S_ISDIR(mode):
            self.data = set()
        else:
            self.data = ''    
    

    def read(self, offset, length):
       return ''
   
    def write(self, offset, data):
       return ''
   
    def truncate(self, length):
       return ''
   