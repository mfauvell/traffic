import copy
from tqdm import tqdm
import traffic_graph.traffic_graph as tg

def prepare_selected(selectedPointsData, mongoDb):
    result = dict()
    preparedCollection = mongoDb['selected_points_prepared_data']
    preparedCollection.drop()
    for selectedPoint, dataPoint in tqdm(selectedPointsData.items()):
        result[selectedPoint] = dict()
        # Get raw data for point
        dataMongoCursor = point_raw_data(selectedPoint, mongoDb)
        # Foreach raw data
        for rawData in dataMongoCursor:
            # Calculate load
            if (int(rawData['intensity']) > int(dataPoint['capacity'])):
                continue
            load = calculate_load(int(dataPoint['capacity']), int(rawData['intensity']), int(rawData['occupation']))
            ## merge meteo_data
            meteoData = get_meteo_data(rawData['meteoMeasures'], dataPoint['meteo_code_sorted'])
            if (len(meteoData) < 1):
                continue
            rawData = rawData | meteoData
            rawData['load'] = load
            ## clean data
            rawData = clean_row(rawData)
            ## save to result
            result[selectedPoint][rawData['date']] = rawData
            ## save to mongo
            preparedCollection.insert_one(rawData)
    return result


def clean_row(rawData):
    meteoIdMeasures = ['80','81','82','83','86','87','88','89']
    newRow = dict()
    newRow['point_id'] = rawData['point_id']
    newRow['date'] = rawData['date']
    newRow['year'] = rawData['year']
    newRow['month'] = rawData['month']
    newRow['day'] = rawData['day']
    newRow['hour'] = rawData['hour']
    newRow['minute'] = int(rawData['minute'])
    newRow['weekday'] = rawData['weekday']
    newRow['day_type'] = rawData['day_type']
    newRow['season'] = rawData['season']
    newRow['prematch'] = rawData['prematch']
    newRow['match'] = rawData['match']
    newRow['postmatch'] = rawData['postmatch']
    newRow['audience'] = rawData['audience']
    newRow['place'] = rawData['place']
    newRow['bank_holiday'] = rawData['bank_holiday']
    newRow['work_office_day'] = rawData['work_office_day']
    newRow['school_day'] = rawData['school_day']
    newRow['school_holiday'] = rawData['school_holiday']
    newRow['state_of_alarm'] = rawData['state_of_alarm']
    newRow['intensity'] = int(rawData['intensity'])
    newRow['occupation'] = int(rawData['occupation'])
    newRow['load'] = rawData['load']
    for id in meteoIdMeasures:
        if id in rawData:
            newRow[tg.data_loading.get_name_of_measure_id(id)] = rawData[id]
    return newRow

def calculate_load(capacity, intensity, ocupation):
    # C = Y + (1-Y) X TO where Y = intensity / capacity and TO = ocupation / 100
    Y = intensity / capacity
    TO = ocupation / 100 
    return round( Y + (1 - Y) * TO, 2)

def point_raw_data(point, mongoDb):
    pipeline = [
        {
            '$match': {
                '$and': [
                    {
                        'error': {
                            '$eq': 'N'
                        }
                    }, {
                        'point_id': point
                    }, {
                        '$or': [
                            {
                                'date': {
                                    '$gt': '2021-06-21 00:00:00'
                                }
                            }, {
                                'date': {
                                    '$lt': '2020-03-14 00:00:00'
                                }
                            }
                        ]
                    }
                ]
            }
        }, {
            '$sort': {
                'date': +1
            }
        }, {
            '$lookup': {
                'from': 'calendar', 
                'localField': 'date', 
                'foreignField': 'date', 
                'pipeline': [
                    {
                        '$lookup': {
                            'from': 'meteo_measures', 
                            'localField': 'date', 
                            'foreignField': 'date',
                            'as': 'meteoMeasures'
                        }
                    }, {
                        '$addFields': {
                            'meteoMeasures': {
                                '$arrayToObject': {
                                    '$map': {
                                        'input': '$meteoMeasures', 
                                        'in': {
                                            'k': {
                                                '$toString': '$$this.code_station'
                                            }, 
                                            'v': '$$this'
                                        }
                                    }
                                }
                            }
                        }
                    }
                ], 
                'as': 'calendar'
            }
        }, {
            '$replaceRoot': {
                'newRoot': {
                    '$mergeObjects': [
                        '$$ROOT', {
                            '$arrayElemAt': [
                                '$calendar', 0
                            ]
                        }
                    ]
                }
            }
        }, {
            '$unset': [
                'tp', 'calendar'
            ]
        }
    ]
    return mongoDb['traffic_measures'].aggregate(pipeline)

def get_meteo_data(rawData, pointsOrdered):
    result = dict()
    # create array of typeValues
    typeValues = ["80", "81", "82", "83", "86", "87", "88", "89"]
    meteoPoints = copy.deepcopy(pointsOrdered)
    while (len(meteoPoints) > 0 and len(typeValues) > 0):
        workingMeteoPoint = meteoPoints.pop(0)
        ## get meteo_measure
        meteoMeassure = rawData.get(workingMeteoPoint)
        if (meteoMeassure is not None):
            for type in typeValues:
                typeValue = meteoMeassure.get(type)
                if (typeValue is not None):
                    result[type] = typeValue
                    typeValues.remove(type)
    return result
