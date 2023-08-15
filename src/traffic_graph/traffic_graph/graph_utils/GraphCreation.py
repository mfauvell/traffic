import numpy as np
import pandas as pd
from math import exp
import copy
import requests
from tqdm import tqdm
from .TrafficDataset import TrafficDataset

def get_matrix_distances(selectedPoints, threshold = 10):
    print("Calculation matrix distances:\t")
    # Get for each point the distances to others
    pointsDistances = dict()
    for selectedPoint in selectedPoints:
        pointsDistances[selectedPoint] = get_openroute_matrix_distances(
            selectedPoints[selectedPoint], 
            dict(filter(lambda item : item[0] != selectedPoint, selectedPoints.items()))
            )
    # get distance matrix
    distances = pd.DataFrame(index=list(selectedPoints.keys()),
                             columns=list(selectedPoints.keys()))
    for selectedPoint in tqdm(selectedPoints):
        selectedPointDistances = copy.deepcopy(pointsDistances[selectedPoint])
        while len(selectedPointDistances) > 0:
            # get less distance point and add to matrix until there aren't any more points
            minPoint = min(selectedPointDistances, key=selectedPointDistances.get)
            minDistance = selectedPointDistances[minPoint]
            distances.loc[selectedPoint, minPoint] = minDistance
            selectedPointDistances.pop(minPoint)
            #check all others points to not add
            actualSelectedPointDistancesKeys = list(selectedPointDistances.keys())
            for additionalPoint in actualSelectedPointDistancesKeys:
                # Pass for minpoint
                if (round(selectedPointDistances[additionalPoint],2) == round(minDistance + pointsDistances[minPoint][additionalPoint], 2)):
                    calculatedDistance = round(get_openroute_distance_between_two_points(
                        selectedPoints[selectedPoint],
                        selectedPoints[additionalPoint], 
                        selectedPoints[minPoint]
                        ),2)
                    limit = round(selectedPointDistances[additionalPoint] + (selectedPointDistances[additionalPoint] * (threshold/100)),2)
                    if (calculatedDistance <= limit):
                        distances.loc[selectedPoint, additionalPoint] = calculatedDistance
                    selectedPointDistances.pop(additionalPoint)
    return distances

def get_weigth_stack(matrix, weight_threshold):
    std = np.nanstd(matrix.to_numpy().ravel())
    def get_weight(distance):
        return exp(-distance ** 2 / std ** 2)
    weights = matrix.applymap(get_weight).fillna(0).round(4)
    weights_lim = weights[weights > weight_threshold].stack()
    return weights_lim

def create_graph_dataset(weights, selectedPointToIndex):
    nodes_src, nodes_target = zip(*weights.index)
    nodes_src = np.array(nodes_src)
    nodes_target = np.array(nodes_target)
    nodes_src_graph = np.array([selectedPointToIndex[x] for x in nodes_src])
    nodes_target_graph = np.array([selectedPointToIndex[x] for x in nodes_target])
    return TrafficDataset(len(selectedPointToIndex), nodes_src_graph, nodes_target_graph, weights)

def get_openroute_matrix_distances(origin, destinations):
    url = 'http://openroute.local/ors/v2/matrix/driving-car'
    codes = list(destinations.keys())
    locations = list(destinations.values())
    destinations = np.arange(1,len(locations) +1 ,1).tolist()
    locations.insert(0, origin)
    sources = [0]
    response = requests.post(url, json = {
            "locations": locations,
            "destinations": destinations,
            "sources": sources,
            "metrics": ["duration"]
        })
    responseDecoded = response.json()
    distances = responseDecoded['durations'][0]
    result = dict()
    index = 0
    for code in codes:
        result[code] = distances[index]
        index = index + 1
    return result

def get_openroute_distance_between_two_points(origin, destination, exclude):
    url = 'http://openroute.local/ors/v2/directions/driving-car'
    coordinates = [origin, destination, exclude]
    response = requests.post(url, json = {
            "coordinates" : coordinates,
            "instructions": False,
            "skip_segments": [1]
        })
    responseDecoded = response.json()
    return responseDecoded['routes'][0]['summary']['duration']