import pymongo
import traffic_graph.traffic_graph as tg

basePath = "./data"
mongoClient = pymongo.MongoClient("mongodb://root:root@localhost:27017/")
trafficDb = mongoClient['traffic']


mongoClient.drop_database('traffic')
trafficPointIntensity = tg.data_loading.traffic_data_get_max_capacity(
    basePath, trafficDb)
trafficPointAuxCollection = tg.data_loading.traffic_point_load_raw_data(
    basePath, trafficDb)
tg.data_loading.traffic_point_clean_unify_data(
    trafficPointAuxCollection, trafficPointIntensity, trafficDb, 0.000099)
