import pymongo
import traffic_graph.traffic_graph as tg
import pandas as pd

mongoClient = pymongo.MongoClient("mongodb://root:root@localhost:27017/")
trafficDb = mongoClient['traffic']
# Get Config of algorythm and columns
# TODO:
config = dict()
#Get selected points
(selectedPoints, selectedPointsToIndex, _) = tg.data_prepare.get_selected_points(trafficDb)
#Create graph
matrix = tg.graph_utils.get_matrix_distances(selectedPoints)
weights = tg.graph_utils.get_weigth_stack(matrix, 0.1)
graphDataset = tg.graph_utils.create_graph_dataset(weights, selectedPointsToIndex)
graphDataset.process()
#Initialize dates
from_date = '2019-01-01 00:00:00'
to_date = '2023-06-01 00:00:00'
dates = pd.date_range(from_date, to_date, freq="15min")
# for each point get df from mongodb
rawDataCollection = trafficDb['selected_points_prepared_data']
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
    dataFramesPoints[selectedPointId] = tg.data_transform.transform_df(pandaDf, config)
# Create Arrx (data to train) Arry (Data to predict) and RightData variables
# right_time_gaps = (dates.to_series().diff().apply(lambda x: x.total_seconds() / 60) == 15).rolling(2*seq_len).sum() == 2*seq_len
# right_time_gaps = right_time_gaps.shift(-2 * seq_len).fillna(False).reset_index(drop=True)
# right_time_gaps = right_time_gaps[right_time_gaps].index.values
# n_rows = len(right_time_gaps)
# arrx = np.full((n_rows, seq_len, len(ids_list), n_features), np.nan)
# arry = np.full((n_rows, seq_len, len(ids_list), n_features), np.nan)

# get configured gaps
# Combine all df in two df, one with X y other with Y
# Get from X and Y train data
# Get from X and Y test data
# Normalize data
# Load all data in a GraphRNN

