import pymongo
import traffic_graph.traffic_graph as tg

mongoClient = pymongo.MongoClient("mongodb://root:root@localhost:27017/")
trafficDb = mongoClient['traffic']
# Get Config of algorythm and columns
# TODO:
config = dict(
    target="load",
    seq_len = 16,
    from_date = '2019-01-01 00:00:00',
    to_date = '2023-06-01 00:00:00',
    year = 'passthrough',
    month = 'passthrough',
    day = 'passthrough',
    hour = 'passthrough',
    minute = 'passthrough',
    weekday = 'passthrough',
    day_type = 'ordinal',
    season = 'ordinal',
    prematch = 'passthrough',
    match = 'passthrough',
    postmatch = 'passthrough',
    bank_holiday = 'passthrough',
    work_office_day = 'passthrough',
    school_day = 'passthrough',
    school_holiday = 'passthrough',
    state_of_alarm = 'passthrough',
    intensity = 'passthrough',
    occupation = 'passthrough',
    ultraviolet = 'passthrough',
    wind = 'wind_speed',
    temperature = 'passthrough',
    humidity = 'passthrough',
    pressure = 'passthrough',
    radiation = 'passthrough',
    rain = 'passthrough',
)

#Get selected points
(selectedPoints, selectedPointsToIndex, _) = tg.data_prepare.get_selected_points(trafficDb)
#Create graph
matrix = tg.graph_utils.get_matrix_distances(selectedPoints)
weights = tg.graph_utils.get_weigth_stack(matrix, 0.1)
graph = tg.graph_utils.create_graph_dataset(weights, selectedPointsToIndex)
graph.process()

arrx, arry = tg.data_prepare.get_data_dataframes(config, selectedPointsToIndex, trafficDb)
# Get from X and Y train data
# Get from X and Y test data
# Normalize data
# Load all data in a GraphRNN

