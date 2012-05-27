'''
Created on May 16, 2012

@author: anduril
'''
import logging
import sys, time, stat, copy

class Item(object):
    """
    An Item is an Object.
    It has two main elements: attributes and data
    """
    def __init__(self, mode, uid, gid, **kwargs):
        # ----------------------------------- Metadata --
        #self.attrs = dict()
        if 'initial' in kwargs:
            logging.info("Loading __dict__")
            dictionary = kwargs['initial']
            try:
                self.__dict__ = dictionary
                logging.info("__dict__ initialized")
                logging.info(self.__dict__)
            except:
                logging.error(sys.exc_info()[0])
                logging.error("error") 
        else:
            self.atime = time.time()   # time of last acces
            self.mtime = self.atime    # time of last modification
            self.ctime = self.atime    # time of last status change
            self.dev = 0        # device ID (if special file)
            self.mode = mode     # protection and file-type
            self.uid = uid      # user ID of owner
            self.gid = gid      # group ID of owner
            self.datalen = 0
	    self.category = ""
        # ------------------------ Extended Attributes --
            self.xattr = {}

        # --------------------------------------- Data --
            if stat.S_ISDIR(mode):
                self.data = list()
            else:
                self.data = ''
            
    def getMetadata(self):
        ''' Returns Metadata to be stored in mongo '''
        logging.info("Get Metadata")
        meta = copy.deepcopy(self.__dict__)
        if 'data' in meta:
            del meta['data']
        return meta

    def getData(self):
        ''' Returns data to be stored in bsddb '''
        logging.info("Get Data")
        return str(self.data)


