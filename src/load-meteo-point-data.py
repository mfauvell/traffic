import pymongo
import traffic_graph.traffic_graph as tg

basePath = "./data"
mongoClient = pymongo.MongoClient("mongodb://root:root@localhost:27017/")
trafficDb = mongoClient['traffic']

tg.data_loading.meteo_points_load_data(basePath, trafficDb)
tg.data_loading.meteo_points_load_measures(basePath, trafficDb)
tg.data_loading.expand_meteo_meassures(trafficDb)
