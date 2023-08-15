import torch
import dgl
from dgl.data import DGLDataset

class TrafficDataset(DGLDataset):
    def __init__(self, n_nodes, nodes_src_graph, nodes_target_graph, weights):
        self.edges_src = torch.from_numpy(nodes_src_graph)
        self.edges_dst = torch.from_numpy(nodes_target_graph)
        self.n_nodes = n_nodes
        self.weights = weights
        super().__init__(name='traffic_graph')


    def process(self):
        self.graph = dgl.graph((self.edges_src, self.edges_dst), num_nodes=self.n_nodes)
        #self.graph.ndata['feat'] = node_features
        self.graph.edata['weight'] = torch.from_numpy(self.weights.values)

        # If your dataset is a node classification dataset, you will need to assign
        # masks indicating whether a node belongs to training, validation, and test set.

    def __getitem__(self, i):
        return self.graph

    def __len__(self):
        return 1