from building_depot import DataService, BDError
import json
import authdata
import csv

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
			self.doneuuidlist = reader.readrow()
		with open('metadata/uuidlist.csv', 'rb') as fp:
			reader = csv.reader(fp, delimiter=';')
			self.uuidList= reader.readrow()

	def store_entire_uuids(self):
		uuidList = list()
		offsetList = range(0,11998, 1000)
		for offset in offsetList:
			sensors = self.bdDS.list_sensors(query_context={}, offset=offset, limit = 1000)
			sensors = sensors['sensors']
			uuidList = uuidList + [sensor['uuid'] for sensor in sensors]
		with open('metadata/uuidlist.csv', 'wb') as fp:
			writer = csv.writer(fp, delimiter=';')
			writer.writerow(uuidList)
	def store_entire_data(self):
		filename = 'data/rawdata_'
		fileIdx = 0
		uuidCnt = 0
		maxuuidNumInFile = 200
		dataDict = dict()
		for uuid in self.uuidList:
			if not uuid in self.doneList:
				batchQ = dict()
				sensorpoints = bdDS.list_sensorpoints(uuid, offset=0, limit=2)
#				sensorpoints = sensorpoints['sensorpoints']
#				pointsDict = dict()
#				for sensorpoint in sensorpoints:
#					pntName = sensorpoint['description']
#					pointsDict[pntName] = {"start":beginTimeStr, "stop":endTimeStr}
				batchQ[uuid] = {sensorpoint['description']: {'start':beginTimeStr, 'stop':endTimeStr} for sensorpoint in sensorpoints['sensorpoints']}
				data = sefl.bdDS.get_timeseries_datapoints_batch(batchQ, timeout=100)
				dataDict[data.keys()[0]] = data.values[0]
				uuidCnt += 1
				if uuidCnt >= maxuuidNumInFile:
					uuidCnt = 0
					with open(filename+str(fileIdx).n.zfill(3)+'.json', 'wb') as fp:
						json.dump(dataDict, fp)
					dataDict = dict()
					fileIdx += 1

