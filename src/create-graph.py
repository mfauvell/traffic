import pymongo
import traffic_graph.traffic_graph as tg

mongoClient = pymongo.MongoClient("mongodb://root:root@localhost:27017/")
trafficDb = mongoClient['traffic']
#Get selected points
selectedPointsCursor = trafficDb['selected_points'].find({})
selectedPoints = dict()
selectedPointToIndex = dict() # This is because dgl has to use numeric id of points
for index, selectedPoint in enumerate(selectedPointsCursor):
    selectedPoints[selectedPoint['point_id']] = [selectedPoint['longitude'], selectedPoint['latitude']]
    selectedPointToIndex[selectedPoint['point_id']] = index
matrix = tg.graph_utils.get_matrix_distances(selectedPoints)
weights = tg.graph_utils.get_weigth_stack(matrix, 0.1)
#Create graph
graph_dataset = tg.graph_utils.create_graph_dataset(weights, selectedPointToIndex)
graph_dataset.process()
print(graph_dataset.graph)
