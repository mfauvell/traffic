import pymongo
import traffic_graph.traffic_graph as tg

mongoClient = pymongo.MongoClient("mongodb://root:root@localhost:27017/")
trafficDb = mongoClient['traffic']
tg.data_loading.add_meteo_poinst_to_traffic_points_data(trafficDb)
