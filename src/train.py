import pymongo
import traffic_graph.traffic_graph as tg
import datetime
import os
import dgl
import pickle
import pandas as pd

mongoClient = pymongo.MongoClient("mongodb://root:root@localhost:27017/")
trafficDb = mongoClient['traffic']
# Get Config of algorythm and columns
# TODO:
config = dict(
    batch_size=192,
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

#Get selected points
(selectedPoints, selectedPointsToIndex, _) = tg.data_prepare.get_selected_points(trafficDb)
# configs = tg.config.get_configs_train()
configs = dict(c1 = config)
now = datetime.datetime.now()
dataset_name = "train_" + now.strftime("%Y%m%d%H%M%S")
basepath = 'results/'+dataset_name
if not os.path.exists(basepath):
    os.mkdir(basepath)
#foreach config
for name, config in configs.items():
    #Crate directory por config
    training_basepath = basepath + "/" + name
    if not os.path.exists(training_basepath):
        os.mkdir(training_basepath)
    #save config
    with open(f"{training_basepath}/learning_args.pkl", "wb") as f:
        pickle.dump(config, f)
    #Create graph
    graph = tg.graph_utils.get_graph(selectedPoints, selectedPointsToIndex, config['graph_threshold'], config['graph_limit_distance'])
    #save graph
    dgl.save_graphs(f"{training_basepath}/graph.bin", [graph])
    #Get base data
    arrx, arry, time_gaps, dates = tg.data_prepare.get_data_dataframes(config, selectedPointsToIndex, trafficDb)
    #save dataframes
    #TODO: For now no save this, we can recreate from data
    for train_time in pd.date_range("2022-07-1", "2023-06-30", freq="1M"):
        path = training_basepath + "/" + train_time.strftime("%Y%m%d%H%M%S")
        if not os.path.exists(path):
            os.mkdir(path)
        try:
            tg.train_utils.make_train(arrx, arry, graph, time_gaps, dates, train_time.strftime("%Y-%m-%d %H:%M:%S"), config, path)
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")





