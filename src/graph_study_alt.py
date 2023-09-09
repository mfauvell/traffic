import pymongo
import traffic_graph.traffic_graph as tg
import datetime
import pandas as pd
import pickle
import os
import dgl
import sys

mongoClient = pymongo.MongoClient("mongodb://root:root@localhost:27017/")
trafficDb = mongoClient['traffic']
# Get Config of algorythm and columns
logPath = '/home/fauvell/Projectes/TFM/traffic/src/results/log.txt'
configId = 'c' + sys.argv[1]
fold = int(sys.argv[2])
iterationName = sys.argv[3]
now = datetime.datetime.now()
with open(logPath, 'a') as f:
    f.write('Initiating Training: ' + iterationName + '\n')
    f.write('Time: ' + now.strftime("%Y-%m-%d %H:%M:%S") +' \n')
    f.write('Config: ' + configId + ' Fold: ' + str(fold) + ' Time: ' + tg.data_prepare.get_training_date_from_fold(fold) +'\n')
#Get selected points
(selectedPoints, selectedPointsToIndex, _) = tg.data_prepare.get_selected_points(trafficDb)
configs = tg.config.get_configs_graphs_study()
dataset_name = "graphStudy_" + iterationName
basepath = '/home/fauvell/Projectes/TFM/traffic/src/results/'+dataset_name
if not os.path.exists(basepath):
    os.mkdir(basepath)
config = configs[configId]
#Crate directory por config
training_basepath = basepath + "/" + configId
if not os.path.exists(training_basepath):
    os.mkdir(training_basepath)
#save config
with open(f"{training_basepath}/learning_args.pkl", "wb") as f:
    pickle.dump(config, f)
#Create graph
with open(logPath, 'a') as f:
    f.write('Creating graph\n')
graph = tg.graph_utils.get_graph(selectedPoints, selectedPointsToIndex, config['graph_threshold'], config['graph_limit_distance'])
#save graph
dgl.save_graphs(f"{training_basepath}/graph.bin", [graph])
#Get base data
with open(logPath, 'a') as f:
    f.write('Get data\n')
arrx, arry, time_gaps, dates = tg.data_prepare.get_data_dataframes(config, selectedPointsToIndex, trafficDb)
#save dataframes
#TODO: For now no save this, we can recreate from data
path = training_basepath + "/fold" + str(fold) 
if not os.path.exists(path):
    os.mkdir(path)
try:
    # dcrnn = tg.train_utils.make_train_alt(arrx, arry, graph, fold, config, path, logPath)
    dcrnn = tg.train_utils.make_train(arrx, arry, graph, time_gaps, dates, tg.data_prepare.get_training_date_from_fold(fold), config, path, logPath)
except Exception as err:
    print(f"Unexpected {err=}, {type(err)=}")
    with open(logPath, 'a') as f:
        f.write(err)