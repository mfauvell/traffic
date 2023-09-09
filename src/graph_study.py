import pymongo
import traffic_graph.traffic_graph as tg
import datetime
import pandas as pd
import pickle
import os
import dgl
import copy
import torch

mongoClient = pymongo.MongoClient("mongodb://root:root@localhost:27017/")
trafficDb = mongoClient['traffic']
# Get Config of algorythm and columns

#Get selected points
(selectedPoints, selectedPointsToIndex, _) = tg.data_prepare.get_selected_points(trafficDb)
configs = tg.config.get_configs_graphs_study()
now = datetime.datetime.now()
dataset_name = "graphStudy_" + now.strftime("%Y%m%d%H%M%S")
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
    for train_time in pd.date_range("2022-07-01", "2023-04-30", freq="1M"):
        path = training_basepath + "/" + train_time.strftime("%Y%m%d%H%M%S")
        if not os.path.exists(path):
            os.mkdir(path)
        try:
            iterationGraph = copy.deepcopy(graph)
            dcrnn = tg.train_utils.make_train(arrx, arry, iterationGraph, time_gaps, dates, train_time.strftime("%Y-%m-%d %H:%M:%S"), config, path)
            # del dcrnn
            # del iterationGraph
            # torch.cuda.empty_cache()
        except Exception as err:
            print(f"Unexpected {err=}, {type(err)=}")
    del arrx
    del arry


