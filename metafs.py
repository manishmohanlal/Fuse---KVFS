import os, sys, stat, errno, time
import json
import fuse
import logging
from bsddb_wrapper import *
import ast
LOG_FILENAME = 'log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)

class Item(object):
    """
    An Item is an Object on Disk, it can be a Directory, File, Symlink, ...
    """
    def __init__(self, mode, uid, gid):
        # ----------------------------------- Metadata --
	
        self.atime = time.time()   # time of last acces
        self.mtime = self.atime    # time of last modification
        self.ctime = self.atime    # time of last status change

        self.dev  = 0        # device ID (if special file)
        self.mode = mode     # protection and file-type
        self.uid  = uid      # user ID of owner
        self.gid  = gid      # group ID of owner

        # ------------------------ Extended Attributes --
        self.xattr = {}

        # --------------------------------------- Data --
        if stat.S_ISDIR(mode):
            self.data = list()
        else:
            self.data = ''
	
    def convertJSON(self):
	return json.dumps(self.__dict__)

def read(data, offset, length):
    logging.info("data")
    return data[offset:offset+length]


def truncate(data, length):
    if len(data) > length:
	data = data[:length]
    else:
        data += '\x00'# (length - len(self.data))
    return data


def write(item, offset, data):
    logging.info("data is "+data)
    length = len(data)
    item.data = item.data[:offset] + data + item.data[offset+length:]
    return item,length



def zstat(stat):
    logging.info("stat.ino = "+str(stat.st_ino))
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

class MetaFS:
    def __init__(self):
        self.uid = os.getuid()
        self.gid = os.getgid()
	self.kv = bsddbWrapper()
	self.kv.addFile('/', Item(0755 | stat.S_IFDIR, self.uid, self.gid).convertJSON())
	self.kv.commit()

    # --- Metadata -----------------------------------------------------------
    def getattr(self, path):
	logging.info("path = "+path)
        if not self.kv.hasKey(path):
            return -errno.ENOENT
	logging.info("looking up")
        # Lookup Item and fill the stat struct
        item = json.loads(self.kv.get(path))
	logging.info(item.atime)
        st = zstat(fuse.Stat())
        st.st_mode  = item.mode
        st.st_uid   = item.uid
        st.st_gid   = item.gid
        st.st_dev   = item.dev
        st.st_atime = item.atime
        st.st_mtime = item.mtime
        st.st_ctime = item.ctime
        st.st_size  = len(item.data)
        return st

    def chmod(self, path, mode):
        item = json.loads(self.kv.get(path))
        item.mode = mode
	self.kv.updateFile(path,json.dumps(item))
	self.kv.commit()

    def chown(self, path, uid, gid):
        item = json.loads(self.kv.get(path))
        item.uid = uid
        item.gid = gid
	self.kv.updateFile(path,json.dumps(item))
	self.kv.commit()

    def utime(self, path, times):
        item = json.loads(self.kv.get(path))
        item.ctime = item.mtime = times[0]
	self.kv.updateFile(path,json.dumps(item))
	self.kv.commit()

    # --- Namespace ----------------------------------------------------------
    def unlink(self, path):
        self._remove_from_parent_dir(path)
	self.kv.deleteFile(path)
	self.kv.commit()

    def rename(self, oldpath, newpath):
        item = self.kv.get(oldpath)
	self.kv.deleteFile(oldpath)
	self.kv.updateFile(newpath,item)
	self.kv.commit()


    # --- Links --------------------------------------------------------------
    def symlink(self, path, newpath):
        item = Item(0644 | stat.S_IFLNK, self.uid, self.gid)
        item.data = path
	self.kv.addFile(newpath,item.convertJSON())
	self.kv.commit()
        self._add_to_parent_dir(newpath)

    def readlink(self, path):
        return json.loads(self.kv.get(path)).data

    # --- Extra Attributes ---------------------------------------------------
    def setxattr(self, path, name, value, flags):
        item = json.loads(self.kv.get(path))
	item.xattr[name] = value
	self.kv.updateFile(path,json.dumps(item))
	self.kv.commit()
	

    def getxattr(self, path, name, size):
        item = json.loads(self.kv.get(path))
	value = item.xattr['name']
        if size == 0:   # We are asked for size of the value
            return len(value)
        return value

    def listxattr(self, path, size):
        item = json.loads(self.kv.get(path))
        attrs = item.xattr.keys()
        if size == 0:
            return len(attrs) + len(''.join(attrs))
        return attrs

    def removexattr(self, path, name):
        item = json.loads(self.kv.get(path))
	xattrs=item.xattr
        if name in xattrs:
            del name
	item.xattr=xattrs
	self.kv.updateFile(path,json.dumps(item))
	self.kv.commit()


    # --- Files --------------------------------------------------------------
    def mknod(self, path, mode, dev):
	logging.info("path mknod: "+path)
        item = Item(mode, self.uid, self.gid)
        item.dev = dev
	self.kv.addFile(path,item.convertJSON())
	self.kv.commit()
        self._add_to_parent_dir(path)

    def create(self, path, flags, mode):
	self.kv.updateFile(path,Item(mode | stat.S_IFREG, self.uid, self.gid).convertJSON())
	self.kv.commit()
        self._add_to_parent_dir(path)

    def truncate(self, path, len):
        item = json.loads(self.kv.get(path))
        item.data=truncate(str(item.data),len)
	self.kv.updateFile(path,json.dumps(item))
	self.kv.commit()
	

    def read(self, path, size, offset):
	#logging .info("data = "+str(self._storage[path].data))
        item = json.loads(self.kv.get(path))
        return read(str(item.data), offset, size)

    def write(self, path, buf, offset):
	logging.info("write invoked offset = "+str(offset));
	logging.info("write invoked data = "+str(buf));
        item = json.loads(self.kv.get(path))
	item,bytesWritten = write(item,offset, buf)
	self.kv.updateFile(path,json.dumps(item))
	self.kv.commit()
        return bytesWritten

    # --- Directories --------------------------------------------------------
    def mkdir(self, path, mode):
	self.kv.addFile(path,Item(mode | stat.S_IFDIR, self.uid, self.gid).convertJSON())
	self.kv.commit()
        self._add_to_parent_dir(path)

    def rmdir(self, path):
        item = json.loads(self.kv.get(path))
        if item.data:
            return -errno.ENOTEMPTY

	self.kv.deleteFile(path)
	self.kv.commit()

    def readdir(self, path, offpathset):
        dir_items = json.loads(self.kv.get(path)).data
	logging.info("readdir")
	logging.info(dir_items)
        for item in dir_items:
	    logging.info(item)
            yield fuse.Direntry(str(item))

    def _add_to_parent_dir(self, path):
	logging.info("adding dir path = "+path)
        parent_path = os.path.dirname(path)
        filename = os.path.basename(path)
	item = json.loads(self.kv.get(parent_path))
	
	logging.info(item)
	logging.info(item.data)
	
	if filename not in item.data:
		logging.info(item.data)
		item.data.append(filename)
		logging.info(item.data)
		self.kv.updateFile(parent_path,json.dumps(item))
		self.kv.commit()
 
    def _remove_from_parent_dir(self, path):
        parent_path = os.path.dirname(path)
        filename = os.path.basename(path)
        item = json.loads(self.kv.get(path))
	if filename in item.data:
		item.data=item.data.remove(filename)
		self.kv.updateFile(path,json.dumps(item))
		self.kv.commit()



