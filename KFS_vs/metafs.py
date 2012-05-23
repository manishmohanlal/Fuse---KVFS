import os, sys, stat, errno, time
import cPickle
import fuse
import logging
from bsddb_wrapper import *
from mongo_wrapper import *
import ast
import mycrypto
LOG_FILENAME = 'log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)

class Item(object):
    """
    An Item is an Object.
    It has two main elements: attributes and data
    """
    def __init__(self, mode, uid, gid):
        # ----------------------------------- Metadata --
	self.attrs = dict()
	
        self.attrs['atime'] = time.time()   # time of last acces
        self.attrs['mtime'] = self.attrs['atime']    # time of last modification
        self.attrs['ctime'] = self.attrs['atime']    # time of last status change

        self.attrs['dev']  = 0        # device ID (if special file)
        self.attrs['mode'] = mode     # protection and file-type
        self.attrs['uid']  = uid      # user ID of owner
        self.attrs['gid']  = gid      # group ID of owner

        # ------------------------ Extended Attributes --
        self.attrs['xattr'] = {}

        # --------------------------------------- Data --
        if stat.S_ISDIR(mode):
            self.data = list()
        else:
            self.data = ''
	
	self.attrs['datalen'] = 0	


    def getMetadata(self):
	''' Returns Metadata to be stored in mongo '''
	logging.info("getMetadat")
	return self.attrs

    def getData(self):
	''' Returns data to be stored in bsddb '''
	logging.info("getData")
	return str(self.data)



''' --------------- Helper Functions ---------------------------'''
def read(data, offset, length):
    logging.info("data")
    content = data[offset:offset+length]
    logging.info(len(content))
    
    #key = "my little key"
    #content = mycrypto.decrypt_string(content, key)
    logging.info(len(content))
    return content


def truncate(data, length):
    if len(data) > length:
	data = data[:length]
    else:
        data += '\x00'# (length - len(self.data))
    return data

def write(newcontent, offset, data):
    logging.info("data is "+data)
    key = "my little key"
    length = len(data)
    #data = mycrypto.encrypt_string(data, key)
    newcontent = newcontent[:offset] + data + newcontent[offset+length:]
    
    return newcontent,length

def zstat(stat):
    logging .info("stat.ino = "+str(stat.st_ino))
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




'''---------------------Actual Metafs class --------------------------------'''
class MetaFS:
    def __init__(self):
	'''initialize and insert root entry'''
        self.uid = os.getuid()
        self.gid = os.getgid()
	self.kv = bsddbWrapper()
	self.mongo = mongoWrapper()
	item = Item(0755 | stat.S_IFDIR, self.uid, self.gid)
	self.mongo.addFile('/', item.getMetadata())
	self.kv.addFile('/', item.getData())
	self.kv.commit()

    # --- Metadata -----------------------------------------------------------
    def getattr(self, path):
	'''Get attributes, returns error if path(key) does not exist'''
	logging.info("path = "+path)
	logging.info("getattr")

        item = self.mongo.get(path)
	if item == None:
		return -errno.ENOENT
        st = zstat(fuse.Stat())
        st.st_mode  = item['mode']
        st.st_uid   = item['uid']
        st.st_gid   = item['gid']
        st.st_dev   = item['dev']
        st.st_atime = item['atime']
        st.st_mtime = item['mtime']
        st.st_ctime = item['ctime']
        st.st_size  = item['datalen']
        return st

    def chmod(self, path, mode):
	item = self.mongo.get(path)
	if item == None:
		return
        item['mode'] = mode
	self.mongo.updateFile(path,item)

    def chown(self, path, uid, gid):
        item = self.mongo.get(path)
	if item == None:
		return
        item['uid'] = uid
        item['gid'] = gid
	self.mongo.updateFile(path,item)

    def utime(self, path, times):
        item = self.mongo.get(path)
	if item == None:
		return
        item['ctime'] = item['mtime'] = times[0]
	self.mongo.updateFile(path,item)

    # --- Namespace ----------------------------------------------------------
    def unlink(self, path):
	'''Removes the file and also remove the entry in parent'''
        self._remove_from_parent_dir(path)
	self.mongo.deleteFile(path)
	self.kv.deleteFile(path)
	self.kv.commit()

    def rename(self, oldpath, newpath):
	'''Replace the oldfilename(key) with new file name'''
        item = self.mongo.get(oldpath)
	data = self.kv.get(oldpath)
	self.mongo.deleteFile(oldpath)
	self.kv.deleteFile(oldpath)
	self.mongo.updateFile(newpath,item)
	self.kv.updateFile(newpath,data)
	self.kv.commit()


    # --- Links --------------------------------------------------------------
    def symlink(self, path, newpath):
	'''Create sym link where data is the path of link'''
        item = Item(0644 | stat.S_IFLNK, self.uid, self.gid)
        item.attrs['datalen'] = len(path)
        item.data = path
	self.mongo.addFile(newpath,item.getMetadata())
	self.kv.addFile(newpath,item.getData())
	self.kv.commit()
        self._add_to_parent_dir(newpath)

    def readlink(self, path):
	return self.mongo.get(path)

    # --- Extra Attributes ---------------------------------------------------
    def setxattr(self, path, name, value, flags):
	item = self.mongo.get(path)
	if item == None:
		return
	item['xattr'][name] = value
	self.mongo.updateFile(path,item)

    def getxattr(self, path, name, size):
	item = self.mongo.get(path)
	if item == None:
		return
	value = item['xattr']['name']
        if size == 0:   # We are asked for size of the value
            return len(value)
        return value

    def listxattr(self, path, size):
	item = self.mongo.get(path)
	if item == None:
		return
        attrs = item['xattr'].keys()
        if size == 0:
            return len(attrs) + len(''.join(attrs))
        return attrs

    def removexattr(self, path, name):
	item = self.mongo.get(path)
	if item == None:
		return
	xattrs=item['xattr']
        if name in xattrs:
            del name
	item['xattr']=xattrs
	self.mongo.updateFile(path,item)


    # --- Files --------------------------------------------------------------
    def mknod(self, path, mode, dev):
	logging.info("path mknod: "+path)
        item = Item(mode, self.uid, self.gid)
        item.attrs['dev'] = dev
	self.mongo.addFile(path,item.getMetadata())
	self.kv.addFile(path,item.getData())
	self.kv.commit()
        self._add_to_parent_dir(path)

    def create(self, path, flags, mode):
	item = Item(mode | stat.S_IFREG, self.uid, self.gid)
	self.mongo.updateFile(path,item.getMetadata())
	self.kv.updateFile(path,item.getData())
	self.kv.commit()
        self._add_to_parent_dir(path)

    def truncate(self, path, len):
        data = self.kv.get(path)
	item = self.mongo.get(path)
	if data == None:
		return
	if item == None:
		return
        data=truncate(str(data),len)
	item['datalen']=len(data)
	self.mongo.updateFile(path,item)
	self.kv.updateFile(path,data)
	self.kv.commit()
	

    def read(self, path, size, offset):
        data = self.kv.get(path)
        return read(str(data), offset, size)

    def write(self, path, buf, offset):
	logging.info("write invoked offset = "+str(offset));
	#logging.info("write invoked data = "+str(buf));
	data = self.kv.get(path)
	data,bytesWritten = write(data,offset, buf)
	self.kv.updateFile(path,data)
	self.kv.commit()
	item = self.mongo.get(path)
	item['datalen']=len(data)
	self.mongo.updateFile(path,item)
        return bytesWritten

    # --- Directories --------------------------------------------------------
    def mkdir(self, path, mode):
	item = Item(mode | stat.S_IFDIR, self.uid, self.gid)
	self.mongo.addFile(path,item.getMetadata())
	logging.info("before kv add")
	self.kv.addFile(path,item.getData())
	self.kv.commit()
        self._add_to_parent_dir(path)

    def rmdir(self, path):
        data = self.kv.get(path)
        if data or data=='[]':
            return -errno.ENOTEMPTY
	self.mongo.deleteFile(path)
	self.kv.deleteFile(path)
	self.kv.commit()

    def readdir(self, path, offpathset):
	logging.info("readdir called")
        data = self.kv.get(path)
	dir_items=eval(data)
        for item in dir_items:
            yield fuse.Direntry(str(item))

    def _add_to_parent_dir(self, path):
	logging.info("adding dir path = "+path)
        parent_path = os.path.dirname(path)
        filename = os.path.basename(path)
	data = self.kv.get(parent_path)
	logging.info(data)
	dir_items=eval(data)
	
	if filename not in dir_items:
		logging.info(dir_items)
		dir_items.append(filename)
		logging.info(dir_items)
		self.kv.updateFile(parent_path,str(dir_items))
		self.kv.commit()
		item = self.mongo.get(parent_path)
		item['datalen']=len(str(dir_items))
		self.mongo.updateFile(parent_path,item)
 
    def _remove_from_parent_dir(self, path):
	logging.info("in remove")
        parent_path = os.path.dirname(path)
        filename = os.path.basename(path)
	data = self.kv.get(parent_path)
	logging.info(data)
	dir_items=eval(data)

	if filename in dir_items:
		dir_items.remove(filename)
		logging.info(dir_items)
		self.kv.updateFile(parent_path,str(dir_items))
		self.kv.commit()
		logging.info("done updating")
		item = self.mongo.get(parent_path)
		item['datalen']=len(str(dir_items))
		self.mongo.updateFile(parent_path,item)



