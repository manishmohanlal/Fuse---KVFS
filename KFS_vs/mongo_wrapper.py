import bsddb
import logging
from pymongo import Connection

LOG_FILENAME = 'log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)

class mongoWrapper:
	def __init__(self):
		self.connection = Connection()
		self.db = self.connection.metadb
		self.collection = self.db.metadata
	
	def addFile(self,key,value):
		metacontent = value
		metacontent['_id']=key
		#insert fails if _id exists
		return self.collection.insert(metacontent)

	def updateFile(self,key,value):
		metacontent = value
		metacontent['_id']=key
		#updates, if _id does not exist insert
		return self.collection.save(metacontent)

	def deleteFile(self,key):
		#if key=None deletes all documents
		if key==None:
			return -1
		else:
			return self.collection.remove(key)
	
	#def hasKey(self,key):	
	#	if self.db.has_key(key):
	#		return True
	#	else:
	#		return False
	
	def get(self,key):
		return self.collection.find_one({'_id':key})


	def close(self):
		self.collection.disconnect()

