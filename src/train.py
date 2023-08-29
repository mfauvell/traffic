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
    year = 'drop',
    month = 'drop',
    day = 'drop',
    hour = 'drop',
    minute = 'drop',
    weekday = 'drop',
    day_type = 'drop',
    season = 'drop',
    prematch = 'drop',
    match = 'drop',
    postmatch = 'drop',
    bank_holiday = 'drop',
    work_office_holiday = 'drop',
    school_day = 'drop',
    school_holiday = 'drop',
    state_of_alarm = 'drop',
    intensity = 'drop',
    ocupation = 'drop',
    ultraviolete = 'drop',
    wind = 'drop',
    temperature = 'drop',
    humidity = 'drop',
    pressure = 'drop',
    radiation = 'drop',
    rain = 'drop',
)

#Get selected points
(selectedPoints, selectedPointsToIndex, _) = tg.data_prepare.get_selected_points(trafficDb)
#Create graph
matrix = tg.graph_utils.get_matrix_distances(selectedPoints)
weights = tg.graph_utils.get_weigth_stack(matrix, 0.1)
graph = tg.graph_utils.create_graph_dataset(weights, selectedPointsToIndex)
graph.process()

arrx, arry = tg.data_prepare.get_data_dataframes(config, selectedPoints, trafficDb)

# Get from X and Y train data
# Get from X and Y test data
# Normalize data
# Load all data in a GraphRNN

