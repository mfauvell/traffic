

def get_selected_points(trafficDb):
  pipeline = [
    {
        '$lookup': {
            'from': 'traffic_points', 
            'localField': 'point_id', 
            'foreignField': 'point_id', 
            'as': 'tp'
        }
    }, {
        '$replaceRoot': {
            'newRoot': {
                '$mergeObjects': [
                    '$$ROOT', {
                        '$arrayElemAt': [
                            '$tp', 0
                        ]
                    }, {
                        '$arrayElemAt': [
                            '$maxIntensity', 0
                        ]
                    }
                ]
            }
        }
    }, {
        '$unset': [
            'tp', 'maxIntensity'
        ]
    }
  ]
  selectedPointsCursor = trafficDb['selected_points'].aggregate(pipeline)
  selectedPoints = dict()
  selectedPointsToIndex = dict() # This is because dgl has to use numeric id of points
  selectedPointsData = dict() # All data of point
  for index, selectedPoint in enumerate(selectedPointsCursor):
      selectedPoints[selectedPoint['point_id']] = [selectedPoint['longitude'], selectedPoint['latitude']]
      selectedPointsToIndex[selectedPoint['point_id']] = index
      selectedPoint['graph_id'] = index
      selectedPointsData[selectedPoint['point_id']] = selectedPoint
  return (selectedPoints, selectedPointsToIndex, selectedPointsData)