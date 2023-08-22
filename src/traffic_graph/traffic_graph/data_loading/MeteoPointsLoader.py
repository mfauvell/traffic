import csv
import os
import datetime
import copy
from tqdm import tqdm
from scipy.interpolate import interp1d


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
    meteoMeasureAuxCollection = mongoDb['meteo_measures_aux']
    meteoMeasureAuxCollection.drop()
    meteoMeasureAuxCollection.create_index([("code_station", +1)])
    meteoMeasureAuxCollection.create_index([("date", +1)])
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
                            yearInt = int(row['ANO'])
                            monthInt = int(row['MES'])
                            dayInt = int(row['DIA'])
                            hourInt = int(hour)
                            # Correct day in last hour
                            if (hour == '24'):
                                dateObject = datetime.datetime(yearInt, monthInt, dayInt)
                                dateObject = dateObject + datetime.timedelta(days = 1)
                                yearInt = dateObject.year
                                monthInt = dateObject.month
                                dayInt = dateObject.day
                                hourInt = 0
                                date = dateObject.strftime("%Y-%m-%d %H:%M:%S")
                            else:
                                date = row['ANO']+'-'+row['MES'] + \
                                    '-'+row['DIA']+' '+hour+':00:00'
                            measure = {}
                            measure['code_station'] = row['ESTACION']
                            measure['date'] = date
                            measure['year'] = yearInt
                            measure['month'] = monthInt
                            measure['day'] = dayInt
                            measure['hour'] = hourInt
                            measure['minute'] = 0
                            measure[row['MAGNITUD']] = float(row['H'+hour])
                            measures[measureKey] = measure
            for measure in measures:
                meteoMeasureAuxCollection.insert_one(measures[measure])


def expand_meteo_meassures(mongoDb):
    meteoIdMeasures = ['80','81','82','83','86','87','88','89']
    minutes = ['15', '30', '45']
    meteoMeasureCollection = mongoDb['meteo_measures']
    meteoMeasureCollection.drop()
    meteoMeasureCollection.create_index([("code_station", +1)])
    meteoMeasureCollection.create_index([("date", +1)])
    meteoPointsCollection = mongoDb['meteo_points']
    meteoMeasureAuxCollection = mongoDb['meteo_measures_aux']
    meteoPointsCursor = meteoPointsCollection.find({})
    for meteoPoint in tqdm(meteoPointsCursor):
        meteoMeasureAuxCursor = meteoMeasureAuxCollection.find({'code_station': meteoPoint['code']}).sort("date")
        initial = meteoMeasureAuxCursor.next()
        lastElement = True
        while lastElement:
            try:
                meteoMeasureCollection.insert_one(initial)
                final = meteoMeasureAuxCursor.next()
            except:
                lastElement = False
                continue
            for minute in minutes:
                newEntry = copy.deepcopy(initial)
                newEntry.pop('_id')
                newEntry['minute'] = int(minute)
                dateObject = datetime.datetime(newEntry['year'], newEntry['month'], newEntry['day'], newEntry['hour'], newEntry['minute'])
                newEntry['date'] = dateObject.strftime("%Y-%m-%d %H:%M:%S")
                for id in meteoIdMeasures:
                    if id in initial and id in final:
                        xs = [0,60]
                        ys = [initial[id],final[id]]
                        interp_func = interp1d(xs, ys)
                        newEntry[id] = round(interp_func(minute).item(),2)
                meteoMeasureCollection.insert_one(newEntry)
            initial = final
            
def get_name_of_measure_id(id):
    match id:
        case "80":
            return 'ultraviolet'
        case "81":
            return 'wind'
        case "82":
            return 'wind_direction'
        case "83":
            return 'temperature'
        case "86":
            return "humidity"
        case "87":
            return "pressure"
        case "88":
            return "solar_radiation"
        case "89":
            return "rain"
        case _:
            return ''