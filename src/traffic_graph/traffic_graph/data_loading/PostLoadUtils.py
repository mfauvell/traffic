import numpy as np
import copy
import requests
from tqdm import tqdm

def add_meteo_poinst_to_traffic_points_data(mongoDb):
    url = "http://openroute.local/ors/v2/matrix/foot-walking"
    #first get all meteo points
    meteoPointCollection = mongoDb['meteo_points']
    meteoCursor = meteoPointCollection.find({})
    meteoCoordenates = []
    meteoCodes = []
    for meteoPoint in meteoCursor:
        meteoCoordenates.append([meteoPoint['longitude'], meteoPoint['latitude']])
        meteoCodes.append(meteoPoint['code'])
    trafficPointCollection = mongoDb['traffic_points']
    trafficPointCusor = trafficPointCollection.find({})
    numberMeteoPoints = len(meteoCoordenates)
    destinations = np.arange(1,numberMeteoPoints +1 ,1).tolist()
    sources = [0]
    for trafficPoint in tqdm(trafficPointCusor):
        pointCoordenates = [float(trafficPoint['longitude']), float(trafficPoint['latitude'])]
        locations = copy.deepcopy(meteoCoordenates)
        locations.insert(0, pointCoordenates)
        response = requests.post(url, json = {
            "locations": locations,
            "destinations": destinations,
            "sources": sources,
            "metrics": ["distance"]
        })
        responseDecoded = response.json()
        distances = responseDecoded['distances']
        index = 0
        bestCode = meteoCodes[0]
        actualDistance = distances[0][0]
        codeDistance = {}
        for code in meteoCodes:
            codeDistance[code] = distances[0][index]
            if actualDistance > distances[0][index]:
                actualDistance = distances[0][index]
                bestCode = code
            index = index+1
        sortedMeteoPoints = dict(sorted(codeDistance.items(), key=lambda item: item[1]))
        trafficPoint['meteo_code'] = bestCode
        trafficPointCollection.update_one({"_id": trafficPoint['_id']}, {"$set": {"meteo_code": bestCode, "meteo_code_sorted": list(sortedMeteoPoints.keys())}})