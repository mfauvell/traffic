import pymongo
import traffic_graph.traffic_graph as tg

basePath = "./data"
mongoClient = pymongo.MongoClient("mongodb://root:root@localhost:27017/")
trafficDb = mongoClient['traffic']

tg.data_loading.create_base_calendar(basePath, trafficDb)
 