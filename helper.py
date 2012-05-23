'''
Created on May 16, 2012

@author: anduril
'''
''' --------------- Helper Functions ---------------------------'''
import pycrypt
import mycrypto
import logging 
from item import Item
import ConfigParser

def initialize():
    config = ConfigParser.RawConfigParser()
    config.read('kfs_config.cfg')
    DATAFILE = config.get('KFSConfig', 'dataFile')
    METADATAHOST = config.get('KFSConfig', 'metadataHost')
    METADATAPORT = config.getint('KFSConfig', 'metadataPort')
    LOGFILE = config.get('KFSConfig', 'logFile')
    MODE = config.getint('KFSConfig', 'defaultMode')
    return DATAFILE, METADATAHOST, METADATAPORT, LOGFILE, MODE

def read(data, offset, length):
    rbuf = ""
    newcontent = ""
    logging.info("Helper Function : Reading data")
    logging.info("Length: %s, Offset: %s" % (length, offset))
    content = data[offset:offset+length]
    key = "manish lund hai!"
    #content = pycrypt.decrypt(content, key)
    PAGESIZE = 4096
    while(True):
        rbuf = content[:PAGESIZE]
        rbuf = mycrypto.decrypt_string(rbuf, key)
        newcontent += rbuf
        content = content[PAGESIZE:]
        if len(content) == 0:
            break
     
    logging.info("Length of data read is "+str(len(content)))
    
    #key = "my little key"
    #content = mycrypto.decrypt_string(content, key)
    
    return newcontent


def truncate(data, length):
    if len(data) > length:
        data = data[:length]
    else:
        data += '\x00'# (length - len(self.data))
    return data

def write(olddata, offset, newdata):
    logging.info("new data is "+newdata)
    key = "manish lund hai!"
    bytesWritten = len(newdata)
    data = mycrypto.encrypt_string(newdata, key)
    #data = pycrypt.encrypt(newdata, key)
    newcontent = olddata[:offset] + data + olddata[offset+bytesWritten:]
    totalLength = len(newcontent)
    return newcontent,bytesWritten, totalLength

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

def getItem(dictionary):
    if dictionary is None:
        return None
    item = Item(None, None, None, initial=dictionary)
    return item