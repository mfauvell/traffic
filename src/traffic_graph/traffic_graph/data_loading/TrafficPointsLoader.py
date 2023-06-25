import csv
import os


def traffic_point_load_raw_data(basePath, mongoDb):
    # Load data from traffic points

    #Parameters:
    #   basePath path where de data is
    #   mongoDb db of mongodb

    #Returns:
    #   A mongo collection with raw data
    
    trafficPointAuxCollection = mongoDb['traffic_points_aux']
    for file in os.listdir(basePath):
        path = basePath + '/' + file
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
            

def traffic_point_clean_unify_data(auxCollection, mongoDb, precission):
    trafficPointCollection = mongoDb['traffic_points']
    for point_id in auxCollection.distinct("point_id"):
        print(point_id)
        pointData = auxCollection.find({"point_id" : point_id}).sort("year", -1).sort('month', -1)
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
            if (abs(float(latitude) - float(data['latitude'])) > precission or abs(float(longitude) - float(data['longitude'])) > precission ):
                modified = True
            latitude = data['latitude']
            longitude = data['longitude']
            name = data['name']
            point_cod = data['point_cod']
        newPoint = {
            "point_id" : point_id,
            "point_cod" : point_cod,
            "name": name,
            "latitude": latitude,
            "longitude": longitude,
            "modified": modified
        }
        trafficPointCollection.insert_one(newPoint)