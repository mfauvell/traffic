import pymongo
import traffic_graph.traffic_graph as tg
import datetime
import pandas as pd

mongoClient = pymongo.MongoClient("mongodb://root:root@localhost:27017/")
trafficDb = mongoClient['traffic']
# Get Config of algorythm and columns

#Get selected points
(selectedPoints, selectedPointsToIndex, _) = tg.data_prepare.get_selected_points(trafficDb)
configs = tg.config.get_configs_graphs_study()
dataset_name = "graphStudy_" + datetime.datetime.strftime("%Y%m%d%H%M%S")
basepath = ''
#foreach config
for name, config in configs.items():
    #Crate directory por config
    #TODO:
    #save config
    #TODO:
    #Create graph
    graph = tg.graph_utils.get_graph(selectedPoints, selectedPointsToIndex, config['graph_threshold'], config['graph_limit_distance'])
    #save graph
    #TODO:
    #Get base data
    arrx, arry, time_gaps, dates = tg.data_prepare.get_data_dataframes(config, selectedPointsToIndex, trafficDb)
    #save dataframes
    #TODO:
    for train_time in pd.date_range("2022-08-1", "2023-05-31", freq="1M"):
        tg.train_utils.make_train(arrx, arry, graph, time_gaps, dates, train_time, config)




