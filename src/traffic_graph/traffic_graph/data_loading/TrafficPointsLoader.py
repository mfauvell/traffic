import csv
import os
import xmltodict
from tqdm import tqdm


def traffic_point_load_raw_data(basePath, mongoDb):
    # Load data from traffic points

    # Parameters:
    #   basePath path where de data is
    #   mongoDb db of mongodb

    # Returns:
    #   A mongo collection with raw data

    print("Loading raw traffic point data:\t")
    pathDir = basePath + '/traffic-points'
    trafficPointAuxCollection = mongoDb['traffic_points_aux']
    trafficPointAuxCollection.drop()
    for file in tqdm(os.listdir(pathDir)):
        path = pathDir + '/' + file
        month = file[15:17]
        year = file[18:22]
        with open(path) as file_obj:
            reader_obj = csv.DictReader(file_obj, delimiter=';')
            points = []
            for row in reader_obj:
                if (row['tipo_elem'] == 'URB'):
                    point = {}
                    point['point_id'] = row['id']
                    point['point_cod'] = row['cod_cent']
                    point['name'] = row['nombre']
                    point['latitude'] = row['latitud']
                    point['longitude'] = row['longitud']
                    point['month'] = month
                    point['year'] = year
                    points.append(point)
            trafficPointAuxCollection.insert_many(points)
    return trafficPointAuxCollection


def traffic_point_clean_unify_data(auxCollection, intesityDic, mongoDb, precission):
    print("Clean and unify traffic point data:\t")
    trafficPointCollection = mongoDb['traffic_points']
    trafficPointCollection.drop()
    for point_id in tqdm(auxCollection.distinct("point_id")):
        pointData = auxCollection.find({"point_id": point_id}).sort(
            "year", -1).sort('month', -1)
        latitude = None
        longitude = None
        name = None
        point_cod = None
        modified = False
        for data in pointData:
            if (latitude == None):
                latitude = data['latitude']
            if (longitude == None):
                longitude = data['longitude']
            if (abs(float(latitude) - float(data['latitude'])) > precission or abs(float(longitude) - float(data['longitude'])) > precission):
                modified = True
            latitude = data['latitude']
            longitude = data['longitude']
            name = data['name']
            point_cod = data['point_cod']
        capacity = -1
        if (point_id in intesityDic):
            capacity = intesityDic[point_id]['capacity']
        newPoint = {
            "point_id": point_id,
            "point_cod": point_cod,
            "name": name,
            "latitude": latitude,
            "longitude": longitude,
            "capacity": capacity,
            "modified": modified
        }
        trafficPointCollection.insert_one(newPoint)


def traffic_point_get_max_capacity(basePath, mongoDb):
    print("Calculate max capacity for each point from data collected:\t")
    pathDir = basePath + '/traffic-points-capacity'
    points = {}
    for file in tqdm(os.listdir(pathDir)):
        path = pathDir + '/' + file
        with open(path) as file_obj:
            data = xmltodict.parse(file_obj.read())
            for element in data['pms']['pm']:
                if ('intensidadSat' in element):
                    capacity = element['intensidadSat']
                else:
                    capacity = -1
                point = {}
                if (element['idelem'] in points):
                    point = points[element['idelem']]
                    if (point['capacity'] != capacity):
                        point['changed'] = True
                        if (point['capacity'] < capacity):
                            point['capacity'] = capacity
                    point['values'].append(capacity)
                else:
                    point['point_id'] = element['idelem']
                    point['capacity'] = capacity
                    point['changed'] = False
                    point['values'] = [capacity]
                points[element['idelem']] = point
    collection = mongoDb['traffic_points_capacity']
    collection.drop()
    for point in points:
        collection.insert_one(points[point])
    return points


def traffic_point_load_measures(basePath, mongoDb):
    print("Loading raw traffic measures data:\t")
    pathDir = basePath + '/traffic-measures'
    trafficMeasuresCollection = mongoDb['traffic_measures']
    trafficMeasuresCollection.drop()
    for file in tqdm(os.listdir(pathDir)):
        path = pathDir + '/' + file
        with open(path) as file_obj:
            reader_obj = csv.DictReader(file_obj, delimiter=';')
            meassures = []
            for row in reader_obj:
                if (row['tipo_elem'] == 'URB'):
                    meassure = {}
                    meassure['point_id'] = row['id']
                    meassure['date'] = row['fecha']
                    meassure['intensity'] = row['intensidad']
                    meassure['occupation'] = row['ocupacion']
                    meassure['vmid'] = row['vmed']
                    meassure['error'] = row['error']
                    meassures.append(meassure)
            trafficMeasuresCollection.insert_many(meassures)
    return trafficMeasuresCollection
