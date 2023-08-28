import pymongo
import traffic_graph.traffic_graph as tg

mongoClient = pymongo.MongoClient("mongodb://root:root@localhost:27017/")
trafficDb = mongoClient['traffic']
#Get selected points
(_, _, selectedPointsData) = tg.data_prepare.get_selected_points(trafficDb)
data = tg.data_prepare.prepare_selected(selectedPointsData, trafficDb)
