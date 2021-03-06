from building_depot import DataService, BDError
import re
import json
import authdata
import csv
from collections import OrderedDict
import os

class Aggregator():
	bdDS = None 
	doneuuidList = None
	beginTimeStr = '2013-06-01T00:00:00'
	endTimeStr = '2015-06-01T00:00:00'
	uuidList = None
	
	
	def __init__ (self):
		self.bdDS = DataService(authdata.srcUrlBase, authdata.bdApiKey, authdata.bdUserName)
		with open('metadata/doneuuidlist.csv', 'rb') as fp:
			reader = csv.reader(fp, delimiter=';')
			self.doneuuidList = list()
			for row in reader:
				self.doneuuidList = self.doneuuidList + row
		with open('metadata/uuidlist.csv', 'rb') as fp:
			reader = csv.reader(fp, delimiter=';')
			self.uuidList = list()
			for row in reader:
				self.uuidList = self.uuidList + row

	def store_entire_uuids(self):
		uuidList = list()
		offsetList = range(0,11998, 1000)
		for offset in offsetList:
			sensors = self.bdDS.list_sensors(query_context={}, offset=offset, limit = 1000)
			sensors = sensors['sensors']
			uuidList = uuidList + [sensor['uuid'] for sensor in sensors]
		self.uuidList = uuidList
		with open('metadata/uuidlist.csv', 'wb') as fp:
			writer = csv.writer(fp, delimiter=';')
			writer.writerow(uuidList)

	def allfiles2csv(self):
		files = os.listdir('./data/')
		for filename in files:
			if not 'json' in filename:
				continue
			print filename
			with open('./data/'+filename, 'rb') as fp:
				jsonData = json.load(fp)
				for uuid, val in jsonData.iteritems():
					self.json2csv(uuid, val)
				jsonData = None

	def seriesDict2SeriesSeries(self, raw):
		gKey = lambda b: b.keys()[0]
		gVal = lambda b: b.values()[0]
		times = map(gKey, raw)
		values = map(gVal, raw)
		return [times, values]


	def json2csv(self, uuid, jsonData):
		with open('data/'+uuid+'.csv', 'wb') as fp:
			writer = csv.writer(fp, delimiter=',')
			for key, val in jsonData.iteritems():
				if key=='message' or 'message' in val.keys():
					continue
				print uuid, key
#				gKey = lambda b: b.keys()[0]
#				gVal = lambda b: b.values()[0]
#				times = map(gKey, val['timeseries'])
#				values = map(gVal, val['timeseries'])
				procData = self.seriesDict2SeriesSeries(val['timeseries'])
				writer.writerow([key])
				writer.writerow(procData[0])
				writer.writerow(procData[1])

	def store_entire_data(self):
		filename = 'data/rawdata_'
		fileIdx = 55
		uuidCnt = 0
		maxuuidNumInFile = 50
		dataDict = dict()
		localUuidList = self.uuidList[5500:]
		for uuid in localUuidList:
			print fileIdx, uuidCnt, uuid
			batchQ = dict()
			sensorpoints = self.bdDS.list_sensorpoints(uuid, offset=0, limit=20)
#			sensorpoints = sensorpoints['sensorpoints']
#			pointsDict = dict()
#			for sensorpoint in sensorpoints:
#				pntName = sensorpoint['description']
#				pointsDict[pntName] = {"start":beginTimeStr, "stop":endTimeStr}
			batchQ[uuid] = {sensorpoint['description']: {'start':self.beginTimeStr, 'stop':self.endTimeStr} for sensorpoint in sensorpoints['sensorpoints']}
			data = self.bdDS.get_timeseries_datapoints_batch(batchQ)
			dataDict[data.keys()[0]] = data.values()[0]
			uuidCnt += 1
			if uuidCnt >= maxuuidNumInFile:
				uuidCnt = 0
				with open(filename+str(fileIdx).zfill(3)+'.json', 'wb') as fp:
					json.dump(dataDict, fp)
				dataDict = dict()
				fileIdx += 1


	def store_entire_tag(self):
		tagDict = dict()
		for uuid in self.uuidList:
			tagDict[uuid] = self.bdDS.list_sensor_context(uuid)
		with open('data/tags.json', 'wb') as fp:
			json.dump(tagDict, fp)
