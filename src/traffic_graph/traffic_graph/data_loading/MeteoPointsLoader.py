import csv
import os
from tqdm import tqdm


def meteo_points_load_data(basePath, mongoDb):
    print("Loading meteo point data:\t")
    pathDir = basePath + '/meteo-points'
    meteoPointCollection = mongoDb['meteo_points']
    meteoPointCollection.drop()
    for file in tqdm(os.listdir(pathDir)):
        path = pathDir + '/' + file
        with open(path) as file_obj:
            reader_obj = csv.DictReader(file_obj, delimiter=';')
            points = []
            for row in reader_obj:
                point = {}
                point['code'] = row['CÓDIGO_CORTO']
                point['street'] = row['DIRECCION']
                longitudeRaw = row['LONGITUD'].replace('.', '')
                point['longitude'] = float(longitudeRaw[:2] +
                                           '.' + longitudeRaw[2:])
                latitudeRaw = row['LATITUD'].replace('.', '')
                point['latitude'] = float(latitudeRaw[:2] +
                                          '.' + latitudeRaw[2:])
                point['81'] = True if row['VV (81)'] == 'X' else False
                point['82'] = True if row['DV (82)'] == 'X' else False
                point['83'] = True if row['T (83)'] == 'X' else False
                point['86'] = True if row['HR (86)'] == 'X' else False
                point['87'] = True if row['PB (87)'] == 'X' else False
                point['88'] = True if row['RS (88)'] == 'X' else False
                point['89'] = True if row['P (89)'] == 'X' else False
                points.append(point)
            meteoPointCollection.insert_many(points)


def meteo_points_load_measures(basePath, mongoDb):
    print("Loading meteo measures data:\t")
    pathDir = basePath + '/meteo-measures'
    meteoMeasureCollection = mongoDb['meteo_measures']
    meteoMeasureCollection.drop()
    hours = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11',
             '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24']
    for file in tqdm(os.listdir(pathDir)):
        path = pathDir + '/' + file
        with open(path) as file_obj:
            reader_obj = csv.DictReader(file_obj, delimiter=';')
            measures = {}
            for row in reader_obj:
                for hour in hours:
                    if (row['V'+hour] == 'V'):
                        measureKey = row['ESTACION'] + '_' + \
                            row['ANO']+row['MES']+row['DIA']+hour
                        if (measureKey in measures):
                            measures[measureKey][row['MAGNITUD']
                                                 ] = float(row['H'+hour])
                        else:
                            if (hour == '24'):
                                date = row['ANO']+'-'+row['MES'] + \
                                    '-'+row['DIA']+' '+'23:59:59'
                            else:
                                date = row['ANO']+'-'+row['MES'] + \
                                    '-'+row['DIA']+' '+hour+':00:00'
                            measure = {}
                            measure['code_station'] = row['ESTACION']
                            measure['year'] = row['ANO']
                            measure['month'] = row['MES']
                            measure['day'] = row['DIA']
                            measure['hour'] = hour
                            measure['date'] = date
                            measure[row['MAGNITUD']] = float(row['H'+hour])
                            measures[measureKey] = measure
            for measure in measures:
                meteoMeasureCollection.insert_one(measures[measure])
