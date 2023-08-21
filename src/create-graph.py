import pymongo
import traffic_graph.traffic_graph as tg

mongoClient = pymongo.MongoClient("mongodb://root:root@localhost:27017/")
trafficDb = mongoClient['traffic']
#Get selected points
(selectedPoints, selectedPointsToIndex, _) = tg.date_prepare.get_selected_points(trafficDb)
matrix = tg.graph_utils.get_matrix_distances(selectedPoints)
weights = tg.graph_utils.get_weigth_stack(matrix, 0.1)
#Create graph
graph_dataset = tg.graph_utils.create_graph_dataset(weights, selectedPointsToIndex)
graph_dataset.process()
print(graph_dataset.graph)
