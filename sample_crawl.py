from building_depot import DataService, BDError
import json
import authdata

bdDS = DataService(authdata.srcUrlBase, authdata.bdApiKey, authdata.bdUserName)

sensors = bdDS.list_sensors(query_context={"room":"rm-2150"})['sensors']

sensorDict = dict()
batchQ = dict()
beginTimeStr = '2015-03-01T00:00:00'
endTimeStr = '2015-06-01T00:00:00'

for sensor in sensors:
	uuid = sensor['uuid']
	sensorpoints = bdDS.list_sensorpoints(uuid, offset=0, limit=2)
	pointsDict = dict()
	sensorpoints = sensorpoints['sensorpoints']
	for sensorpoint in sensorpoints:
		pntName = sensorpoint['description']
		pointsDict[pntName] = {"start":beginTimeStr, "stop":endTimeStr}
	batchQ[uuid] = pointsDict

#data = bdm.get_batch_json(batchQ)
data = bdDS.get_timeseries_datapoints_batch(batchQ, timeout=1000)
#print data.content

with open("RM-2150.json", 'wb') as fp:
	json.dump(data, fp)
