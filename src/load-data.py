import pymongo
import traffic_graph.traffic_graph as tg

basePath = "./data/traffic-points"
mongoClient = pymongo.MongoClient("mongodb://root:root@localhost:27017/")
trafficDb = mongoClient['traffic']


mongoClient.drop_database('traffic')
trafficPointAuxCollection = tg.data_loading.traffic_point_load_raw_data(basePath, trafficDb)
tg.data_loading.traffic_point_clean_unify_data(trafficPointAuxCollection, trafficDb, 0.000099)
