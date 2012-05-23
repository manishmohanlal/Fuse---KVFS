import bsddb
import logging


LOG_FILENAME = 'log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)

class bsddbWrapper:
	def __init__(self):
		self.db = bsddb.hashopen('/tmp/filesystem.db', 'c')
	
	def addFile(self,key,value):
		if self.db.has_key(key):
			return -1
		else:
			self.db[key]=value
			return key

	def updateFile(self,key,value):
		logging.info("update")
		logging.info(key)
		logging.info(value)

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

	def get(self,key):
		logging.info("get = "+self.db[key])
		return self.db[key]

	def commit(self):
		self.db.sync()		

	def close(self):
		self.db.close()

	
