import bsddb3
import logging


LOG_FILENAME = 'log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)

class bsddbWrapper:
	def __init__(self, loc):
		self.db = bsddb3.db.DB()
		self.db.open(loc,flags=bsddb3.db.DB_CREATE,dbtype=bsddb3.db.DB_HASH)
	
	def addFile(self,key,value):
		if self.db.has_key(key):
			return -1
		else:
			self.db[key]=value
			return key

	def updateFile(self,key,value):
		logging.info("update")
		self.db[key]=value
		return key

	def deleteFile(self,key):
		if self.db.has_key(key):
			del self.db[key]
			return key
		else:
			return -1

	def hasKey(self,key):	
		if self.db.has_key(key):
			return True
		else:
			return False

	def get(self,key, **kwargs):
		if 'dlen' in kwargs and 'doff' in kwargs:
			dlen = kwargs['dlen']
			doff = kwargs['doff']
			return self.db.get(key,dlen=dlen,doff=doff)
		else:
			return self.db.get(key)

	def commit(self):
		self.db.sync()		

	def close(self):
		self.db.close()

	
