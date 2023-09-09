import pandas as pd
import numpy as np
import traffic_graph.traffic_graph as tg
from datetime import timedelta, datetime
# import pymongoarrow
from pymongoarrow.monkey import patch_all
from tqdm import tqdm
import math

def get_data_dataframes(config, selectedPoints, mongoDb):
    print('Obtain dataframes:')
    patch_all()
    seq_len = config['seq_len']
    dates = pd.date_range(config['from_date'], config['to_date'], freq="15min")
    # for each point get df from mongodb
    rawDataCollection = mongoDb['selected_points_prepared_data']
    dataFramesPoints = dict()
    ## get raw data point into pandas dataframe and transform
    print('Get data into df:')
    for selectedPointId in tqdm(selectedPoints):
        pandaDf = rawDataCollection.find_pandas_all({"point_id": selectedPointId})
        pandaDf['date'] = pd.to_datetime(pandaDf['date'])
        pandaDf.sort_values(by=['date'], inplace=True)
        ## Intersect dates of df with the generals to get the min
        dates = dates.intersection(pandaDf.date)
        dataFramesPoints[selectedPointId] = pandaDf
    # The explain to make a second iteration is why we need remove dates that is faulty row for some point
    # for each pont transfrom df
    print('Transform data of df:')
    for selectedPointId in tqdm(selectedPoints):
        pandaDf = dataFramesPoints[selectedPointId]
        ## remove data is not in dates
        pandaDf = pandaDf[pandaDf.date.isin(dates)]
        ## transform data
        dataFramesPoints[selectedPointId] = tg.data_transform.transform_df(pandaDf, config, config['target'])
    # get configured gaps
    right_time_gaps = (dates.to_series().diff().apply(lambda x: x.total_seconds() / 60) == 15).rolling(2*seq_len).sum() == 2*seq_len
    right_time_gaps = right_time_gaps.shift(-2 * seq_len).fillna(False).reset_index(drop=True)
    right_time_gaps = right_time_gaps[right_time_gaps].index.values
    n_rows = len(right_time_gaps)
    n_features = next(iter(dataFramesPoints.values())).shape[1]
    # Create Arrx (data to train) Arry (Data to predict) and RightData variables
    arrx = np.full((n_rows, seq_len, len(selectedPoints), n_features), np.nan)
    arry = np.full((n_rows, seq_len, len(selectedPoints), n_features), np.nan)
    # Combine all df in two df, one with X y other with Y
    print('Get final result:')
    for id, df in tqdm(dataFramesPoints.items()):
        graph_id = selectedPoints[id]
        dfi = pd.DataFrame(df)
        for i, timestamp in enumerate(right_time_gaps):
            arrx[i, :, graph_id, :] = dfi.iloc[timestamp:timestamp+seq_len]
            arry[i, :, graph_id, :] = dfi.iloc[timestamp+seq_len:timestamp + 2*seq_len]
    print('Finished obtain dataframes.')            
    return (arrx, arry, right_time_gaps, dates)

def get_train_test_arrays(arrx, arry, right_time_gaps, dates, train_date, config):
    train_date = datetime.strptime(train_date, "%Y-%m-%d %H:%M:%S")
    dates_train = (dates.to_series().reset_index(drop=True) <= train_date)
    train_index = np.intersect1d(dates_train[dates_train].index.values, right_time_gaps)
    train_data_size = len(train_index)
    limitTest = train_date + timedelta(days=30)
    print("Test until: " + limitTest.strftime("%Y-%m-%d %H:%M:%S"))
    dates_test = (dates.to_series().reset_index(drop=True) > train_date) & (dates.to_series().reset_index(drop=True) <= limitTest)
    test_index = np.intersect1d(dates_test[dates_test].index.values, right_time_gaps)
    test_data_size = len(test_index)

    return (arrx[:train_data_size], arry[:train_data_size], arrx[train_data_size:train_data_size + test_data_size], arry[train_data_size:train_data_size + test_data_size])

def get_train_test_arrays_alt(arrx, arry, fold, config):
    size, _, _, _ = arrx.shape
    base_size = math.floor(size / 10)
    if (fold == 0):
        return (arrx[base_size:], arry[base_size:], arrx[:base_size], arry[:base_size])
    elif (fold==9):
        return (arrx[:base_size*9], arry[:base_size*9], arrx[base_size*9:], arry[base_size*9:])
    else:
        first_segment_x = arrx[:base_size*fold]
        first_segment_y = arry[:base_size*fold]
        second_segment_x = arrx[base_size*(fold + 1):]
        second_segment_y = arry[base_size*(fold + 1):]
        return (
            np.concatenate((first_segment_x, second_segment_x), axis = 0),
            np.concatenate((first_segment_y, second_segment_y), axis = 0),
            arrx[base_size*fold:base_size*(fold+1)],
            arry[base_size*fold:base_size*(fold+1)]
        )
    

    # return (arrx[:train_data_size], arry[:train_data_size], arrx[train_data_size:train_data_size + test_data_size], arry[train_data_size:train_data_size + test_data_size])

def get_training_date_from_fold(fold):
    match fold:
        case 0:
            return '2022-08-01 00:00:00'
        case 1:
            return '2022-09-01 00:00:00'
        case 2:
            return '2022-10-01 00:00:00'
        case 3:
            return '2022-11-01 00:00:00'
        case 4:
            return '2022-12-01 00:00:00'
        case 5:
            return '2023-01-01 00:00:00'
        case 6:
            return '2023-02-01 00:00:00'
        case 7:
            return '2023-03-01 00:00:00'
        case 8:
            return '2023-04-01 00:00:00'
        case 9:
            return '2023-05-01 00:00:00'
