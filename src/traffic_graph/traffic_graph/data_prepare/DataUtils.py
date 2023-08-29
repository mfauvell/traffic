import pandas as pd
import numpy as np
import traffic_graph.traffic_graph as tg

def get_data_dataframes(config, selectedPoints, mongoDb):
    seq_len = config['seq_len']
    dates = pd.date_range(config['from_date'], config['to_date'], freq="15min")
    # for each point get df from mongodb
    rawDataCollection = mongoDb['selected_points_prepared_data']
    dataFramesPoints = dict()
    ## get raw data point into pandas dataframe and transform
    for selectedPointId in selectedPoints:
        rawData = rawDataCollection.find({"point_id": selectedPointId}).sort("date")
        pandaDf = pd.DataFrame(list(rawData))
        pandaDf['date'] = pd.to_datetime(pandaDf['date'])
        ## Intersect dates of df with the generals to get the min
        dates = dates.intersection(pandaDf.date)
        dataFramesPoints[selectedPointId] = pandaDf
    # The explain to make a second iteration is why we need remove dates that is faulty row for some point
    # for each pont transfrom df
    for selectedPointId in selectedPoints:
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
    n_features = dataFramesPoints[id].shape[1]
    # Create Arrx (data to train) Arry (Data to predict) and RightData variables
    arrx = np.full((n_rows, seq_len, len(selectedPoints), n_features), np.nan)
    arry = np.full((n_rows, seq_len, len(selectedPoints), n_features), np.nan)
    # Combine all df in two df, one with X y other with Y
    for id, df in dataFramesPoints.items():
        graph_id = df.graph_id
        for i, timestamp in enumerate(right_time_gaps):
            arrx[i, :, graph_id, :] = df.iloc[timestamp:timestamp+seq_len]
            arry[i, :, graph_id, :] = df.iloc[timestamp+seq_len:timestamp + 2*seq_len]
    return (arrx, arry)