import pymongo
import traffic_graph.traffic_graph as tg

basePath = "./data"
mongoClient = pymongo.MongoClient("mongodb://root:root@localhost:27017/")
trafficDb = mongoClient['traffic']

# trafficPointAuxCollection = tg.data_loading.traffic_point_load_raw_data(
#     basePath, trafficDb)
# trafficPointCapacity = tg.data_loading.traffic_point_get_max_capacity(
#     basePath, trafficDb)
# tg.data_loading.traffic_point_clean_unify_data(
#     trafficPointAuxCollection, trafficPointCapacity, trafficDb, 0.000099)
tg.data_loading.traffic_point_load_measures(basePath, trafficDb)
