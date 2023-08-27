import torch.nn as nn
from dgl.base import DGLError
import traffic_graph.traffic_graph as tg

class StackedEncoder(nn.Module):
    '''One step encoder unit for hidden representation generation
    it can stack multiple vertical layers to increase the depth.

    Parameter
    ==========
    in_feats : int
        number if input features

    out_feats : int
        number of output features

    num_layers : int
        vertical depth of one step encoding unit

    net : torch.nn.Module
        message passing network for graph computation
    '''

    def __init__(self, in_feats, out_feats, num_layers, net):
        super(StackedEncoder, self).__init__()
        self.in_feats = in_feats
        self.out_feats = out_feats
        self.num_layers = num_layers
        self.net = net
        self.layers = nn.ModuleList()
        if self.num_layers <= 0:
            raise DGLError("Layer Number must be greater than 0! ")
        self.layers.append(tg.model.GraphGRUCell(
            self.in_feats, self.out_feats, self.net))
        for _ in range(self.num_layers-1):
            self.layers.append(tg.model.GraphGRUCell(
                self.out_feats, self.out_feats, self.net))

    # hidden_states should be a list which for different layer
    def forward(self, g, x, hidden_states):
        hiddens = []
        for i, layer in enumerate(self.layers):
            x = layer(g, x, hidden_states[i])
            hiddens.append(x)
        return x, hiddens