import pymongo
import traffic_graph.traffic_graph as tg

mongoClient = pymongo.MongoClient("mongodb://root:root@localhost:27017/")
trafficDb = mongoClient['traffic']
# Get Config of algorythm and columns
# TODO:
config = dict(
    batch_size=256,
    num_workers=0,
    target="load",
    seq_len = 16,
    diffsteps = 2,
    direction = "both",
    out_feats = 64,
    num_layers = 2,
    decay_steps = 2000,
    lr=0.01,
    gpu = 1,
    minimum_lr=2e-6,
    epochs=20,
    max_grad_norm=5.0,
    from_date = '2019-01-01 00:00:00',
    to_date = '2023-06-01 00:00:00',
    test_days_gap=30,
    year = 'passthrough',
    month = 'passthrough',
    day = 'passthrough',
    hour = 'passthrough',
    minute = 'passthrough',
    weekday = 'passthrough',
    day_type = 'drop',
    season = 'drop',
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
    ultraviolet = 'drop',
    wind = 'drop',
    temperature = 'passthrough',
    humidity = 'passthrough',
    pressure = 'passthrough',
    radiation = 'passthrough',
    rain = 'passthrough',
)

dataset_name = "ff"
#foreach config
#Create name of datacreation
#save config
#save graph
#save dataframes
##foreach make train
## save datasets
### foreach epoch
### save model
### save mae and mes

#Get selected points
(selectedPoints, selectedPointsToIndex, _) = tg.data_prepare.get_selected_points(trafficDb)
#Create graph
graph = tg.graph_utils.get_graph(selectedPoints, selectedPointsToIndex)
#Get base data
arrx, arry, time_gaps, dates = tg.data_prepare.get_data_dataframes(config, selectedPointsToIndex, trafficDb)
train_time = "2023-05-01 00:00:00"

tg.train_utils.make_train(arrx, arry, graph, time_gaps, dates, train_time, config)

