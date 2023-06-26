import csv
import os
import xmltodict


def traffic_point_load_raw_data(basePath, mongoDb):
    # Load data from traffic points

    # Parameters:
    #   basePath path where de data is
    #   mongoDb db of mongodb

    # Returns:
    #   A mongo collection with raw data

    pathDir = basePath + '/traffic-points'
    trafficPointAuxCollection = mongoDb['traffic_points_aux']
    for file in os.listdir(pathDir):
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
    trafficPointCollection = mongoDb['traffic_points']
    for point_id in auxCollection.distinct("point_id"):
        print(point_id)
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
        intensity = -1
        if (point_id in intesityDic):
            intensity = intesityDic[point_id]['intensity']
        newPoint = {
            "point_id": point_id,
            "point_cod": point_cod,
            "name": name,
            "latitude": latitude,
            "longitude": longitude,
            "intensity_max": intensity,
            "modified": modified
        }
        trafficPointCollection.insert_one(newPoint)


def traffic_data_get_max_capacity(basePath, mongoDb):
    pathDir = basePath + '/traffic-points-capacity'
    points = {}
    for file in os.listdir(pathDir):
        path = pathDir + '/' + file
        print(path)
        with open(path) as file_obj:
            data = xmltodict.parse(file_obj.read())
            for element in data['pms']['pm']:
                if ('intensidadSat' in element):
                    intensity = element['intensidadSat']
                else:
                    intensity = -1
                point = {}
                if (element['idelem'] in points):
                    point = points[element['idelem']]
                    if (point['intensity'] != intensity):
                        point['changed'] = True
                        if (point['intensity'] < intensity):
                            point['intensity'] = intensity
                    point['values'].append(intensity)
                else:
                    point['point_id'] = element['idelem']
                    point['intensity'] = intensity
                    point['changed'] = False
                    point['values'] = [intensity]
                points[element['idelem']] = point
    collection = mongoDb['traffic_points_intensity']
    for point in points:
        collection.insert_one(points[point])
    return points
