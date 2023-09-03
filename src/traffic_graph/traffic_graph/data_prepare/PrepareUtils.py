import copy
from tqdm import tqdm
import numpy as np
import traffic_graph.traffic_graph as tg
from pymongoarrow.monkey import patch_all
import pandas as pd
from datetime import datetime, timedelta
from scipy.interpolate import interp1d

def prepare_selected(selectedPointsData, mongoDb):
    patch_all()
    preparedCollection = mongoDb['selected_points_prepared_data']
    preparedCollection.drop()
    preparedCollection.create_index([("date", +1)])
    preparedCollection.create_index([("point_id", +1)])
    pointDataframes = dict()
    lastValue = dict()
    trafficMeasuresCollection = mongoDb['traffic_measures']
    for selectedPoint in tqdm(selectedPointsData):
        pandaDf = point_raw_data(selectedPoint, mongoDb)
        pandaDf['date'] = pd.to_datetime(pandaDf['date'])
        pointDataframes[selectedPoint] = pandaDf
        lastValue[selectedPoint] = dict()
    limitTimestamp = datetime.strptime("2023-06-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    rawCalendar = calendar_with_meteo(mongoDb)
    for calendar in tqdm(rawCalendar):
        batchEntries = []
        timestamp = datetime.strptime(calendar['date'], "%Y-%m-%d %H:%M:%S")
        for selectedPoint, dataPoint in selectedPointsData.items():
            actualValue = pointDataframes[selectedPoint].loc[pointDataframes[selectedPoint]['date'] == timestamp]
            intensity = 0
            occupation = 0
            if (len(actualValue) > 0):
                actualValue = actualValue.iloc[0]
                if (len(lastValue[selectedPoint]) == 0):
                    lastValue[selectedPoint] = actualValue
                intensity = actualValue['intensity']
                occupation = actualValue['occupation']
            # Get Interpolate values
            if (len(actualValue) == 0 and len(lastValue[selectedPoint]) > 0):
                seekNextValue = True
                index = 1
                nextValue = dict()
                #Get next date with value
                timestampInside = copy.deepcopy(timestamp)
                while seekNextValue and timestampInside < limitTimestamp:
                    index = index + 1
                    timestampInside = timestampInside + timedelta(minutes=15)
                    nextValue = pointDataframes[selectedPoint].loc[pointDataframes[selectedPoint]['date'] == timestampInside]
                    if (len(nextValue)>0):
                        nextValue = nextValue.iloc[0]
                        seekNextValue = False
                if (len(nextValue) == 0):
                    continue
                intensity = interpolate(lastValue[selectedPoint]['intensity'], nextValue['intensity'], index)
                occupation = interpolate(lastValue[selectedPoint]['occupation'], nextValue['occupation'], index)
            if (int(intensity) > int(dataPoint['capacity'])):
                continue
            lastValue[selectedPoint] = actualValue
            load = calculate_load(int(dataPoint['capacity']), int(intensity), int(occupation))
            meteoData = get_meteo_data(calendar['meteoMeasures'], dataPoint['meteo_code_sorted'])
            if (len(meteoData) < 5):
                continue
            rawData = calendar | meteoData
            rawData.pop('meteoMeasures')
            rawData['point_id'] = dataPoint['point_id']
            rawData['intensity'] = intensity
            rawData['occupation'] = occupation
            rawData['load'] = load
            rawData['graph_id'] = dataPoint['graph_id']
            ## clean data
            rawData = clean_row(rawData)
            ## save to result
            # result[selectedPoint][rawData['date']] = rawData
            ## save to mongo
            # preparedCollection.insert_one(rawData)
            batchEntries.append(rawData)
        if (len(batchEntries)> 0):
            preparedCollection.insert_many(batchEntries)
    #Get Calendar with Meteo points data
    #For each selected point get raw_data into pandas
    
    return True

def interpolate(firstValue, lastValue, index):
    xs = [0,index]
    ys = [firstValue,lastValue]
    interp_func = interp1d(xs, ys)
    return round(interp_func(1).item(),0)

def clean_row(rawData):
    # meteoIdMeasures = ['80','81','82','83','86','87','88','89']
    meteoIdMeasures = ['83','86','87','88','89']
    newRow = dict()
    newRow['point_id'] = rawData['point_id']
    newRow['graph_id'] = rawData['graph_id']
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
    if 'wind' in newRow and 'wind_direction' in newRow:
        newRow['windx'], newRow['windy'] = convert_wind_columns(newRow['wind_direction'], newRow['wind'])
        newRow.pop('wind_direction')
    return newRow

def convert_wind_columns(direction, velocity):
    wd_rad = direction * np.pi / 180
    windx = velocity * np.cos(wd_rad)
    windy = velocity * np.sin(wd_rad)
    return (windx, windy)

def calculate_load(capacity, intensity, ocupation):
    # C = Y + (1-Y) X TO where Y = intensity / capacity and TO = ocupation / 100
    Y = intensity / capacity
    TO = ocupation / 100 
    return round( Y + (1 - Y) * TO, 2)

def calendar_with_meteo(mongoDb):
    pipeline = [
        {
            '$sort': {
                'date': +1
            }
        }, {
            '$match': {
                '$or': [
                    {
                        '$and': [
                            {
                                'date': {
                                    '$gt': '2021-06-21 00:00:00'
                                }
                            }, {
                                'date': {
                                    '$lt': '2023-06-01 00:00:00'
                                }
                            }
                        ]
                    }, {
                        'date': {
                            '$lt': '2020-03-14 00:00:00'
                        }
                    }
                ]
            }
        }, {
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
    ]
    return mongoDb['calendar'].aggregate(pipeline)

def point_raw_data(point, mongoDb):
    pipeline = [
        {
            '$sort': {
                'date': +1
            }
        }, {
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
        }
    ]
    return mongoDb['traffic_measures'].aggregate_pandas_all(pipeline)

def get_meteo_data(rawData, pointsOrdered):
    result = dict()
    # create array of typeValues
    # typeValues = ["80", "81", "82", "83", "86", "87", "88", "89"]
    typeValues = ['83','86','87','88','89']
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
