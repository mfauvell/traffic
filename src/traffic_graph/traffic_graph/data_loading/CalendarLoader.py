import csv
import os
from tqdm import tqdm
import calendar
import copy
import numpy

def create_base_calendar(basePath, mongoDb):
    print("Creating base calendar:\t")
    filePath = basePath + '/calendar_raw.csv'
    calendarAuxCollection = mongoDb['calendar_aux']
    calendarAuxCollection.drop()
    calendarCollection = mongoDb['calendar']
    calendarCollection.drop()
    with open(filePath) as file_obj:
        reader_obj = csv.DictReader(file_obj, delimiter=';')
        for row in reader_obj:
            calendarAuxCollection.insert_one(row)
    years = [2019, 2020, 2021, 2022, 2023]
    months = range(1, 13)
    hours = range(0, 24)
    minutes = ['00', '15', '30', '45']
    calendarInstance = calendar.Calendar()
    defaultMoment = {
        "date": None,
        "year": None,
        "month": None,
        "day": None,
        "hour": None,
        "minute": None,
        "weekday": None,
        "day_type": None,
        "season": None,
        "prematch": False,
        "match" : False,
        "postmatch": False,
        "audience": 0,
        "place": None,
        "bank_holiday": False,
        "work_office_day": False,
        "school_day": False,
        "school_holiday": False,
        "state_of_alarm": False
    }
    for year in years:
        print('Year: ' + str(year))
        for month in months:
            print('Month: ' + str(month))
            for day in tqdm(calendarInstance.itermonthdays4(year, month)):
                if (day[1] == month):
                    for hour in hours:
                        for minute in minutes:
                            date = str(year) + '-' + str(month).zfill(2) + '-' + str(
                                day[2]).zfill(2) + ' ' + str(hour).zfill(2) + ':' + minute + ':00'
                            query = {"start": {"$lte": date},
                                     "end": {"$gt": date}}
                            result = calendarAuxCollection.find(query)
                            result =list(result)
                            weekday = day[3]
                            moment = copy.deepcopy(defaultMoment)
                            moment['date'] = date
                            moment['year'] = year
                            moment['month'] = month
                            moment['day'] = day[2]
                            moment['hour'] = hour
                            moment['minute'] = minute
                            moment['weekday'] = weekday
                            moment['day_type'] = day_type(weekday).item(0)
                            moment['season']= get_season(month, day[2]).item(0)
                            prematch = [d for d in result if d['type'] == 'prematch']
                            if len(prematch) > 0:
                                moment['prematch'] = True
                                moment['audience'] = prematch[0]['watchers']
                                moment['place'] = prematch[0]['place']
                            match = [d for d in result if d['type'] == 'match']
                            if len(match) > 0:
                                moment['match'] = True
                                moment['audience'] = match[0]['watchers']
                                moment['place'] = match[0]['place']
                            postmatch = [d for d in result if d['type'] == 'postmatch']
                            if len(postmatch) > 0:
                                moment['postmatch'] = True
                                moment['audience'] = postmatch[0]['watchers']
                                moment['place'] = postmatch[0]['place']
                            bankHoliday = [d for d in result if d['type'] == 'bankHoliday']
                            moment['bank_holiday'] = len(bankHoliday) > 0
                            moment['work_office_day'] = len(bankHoliday) == 0 and weekday < 5
                            schoolHoliday = [d for d in result if d['type'] == 'schoolHoliday']
                            moment['school_holiday'] = len(schoolHoliday) > 0
                            moment['school_day'] = len(schoolHoliday) == 0 and weekday < 5
                            moment['state_of_alarm'] = len([d for d in result if d['type'] == 'pandemic']) > 0
                            calendarCollection.insert_one(moment)


def day_type(weekday):
    conditions = [
        weekday == 6,
        weekday < 5,
        weekday == 5
    ]

    day_types = [
        "sun",
        "mon-fri",
        "sat"
    ]

    return numpy.select(conditions, day_types, default=numpy.nan)


def get_season(month, day):
    conditions = [
        (month <= 3) & (day <= 21),
        (month <= 6) & (day <= 21),
        (month <= 9) & (day <= 22),
        (month <= 12) & (day <= 21),
        (month <= 12) & (day <= 31)
    ]
    seasons = [
        "winter",
        "spring",
        "summer",
        "fall",
        "winter"
    ]
    return numpy.select(conditions, seasons)
