import pandas as pd
import numpy as np
import traffic_graph.traffic_graph as tg
from datetime import timedelta, datetime
# import pymongoarrow
from pymongoarrow.monkey import patch_all
from tqdm import tqdm

def get_data_dataframes(config, selectedPoints, mongoDb):
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
            
    return (arrx, arry, right_time_gaps, dates)

def get_train_test_arrays(arrx, arry, right_time_gaps, dates, train_date, config):
    print(arrx.shape[0])
    train_date = datetime.strptime(train_date, "%Y-%m-%d %H:%M:%S")
    dates_train = (dates.to_series().reset_index(drop=True) <= train_date)
    train_index = np.intersect1d(dates_train[dates_train].index.values, right_time_gaps)
    train_data_size = len(train_index)
    print(train_data_size)
    limitTest = train_date + timedelta(days=30)
    print(limitTest.strftime("%Y-%m-%d %H:%M:%S"))
    dates_test = (dates.to_series().reset_index(drop=True) > train_date) & (dates.to_series().reset_index(drop=True) <= limitTest)
    test_index = np.intersect1d(dates_test[dates_test].index.values, right_time_gaps)
    test_data_size = len(test_index)
    print(test_data_size)

    return (arrx[:train_data_size], arry[:train_data_size], arrx[train_data_size:train_data_size + test_data_size], arry[train_data_size:train_data_size + test_data_size])

