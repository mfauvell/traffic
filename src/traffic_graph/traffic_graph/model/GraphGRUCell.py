import torch
import torch.nn as nn


class GraphGRUCell(nn.Module):
    '''Graph GRU unit which can use any message passing
    net to replace the linear layer in the original GRU
    Parameter
    ==========
    in_feats : int
        number of input features

    out_feats : int
        number of output features

    net : torch.nn.Module
        message passing network
    '''

    def __init__(self, in_feats, out_feats, net):
        super(GraphGRUCell, self).__init__()
        self.in_feats = in_feats
        self.out_feats = out_feats
        self.dir = dir
        # net can be any GNN model
        self.r_net = net(in_feats+out_feats, out_feats)
        self.u_net = net(in_feats+out_feats, out_feats)
        self.c_net = net(in_feats+out_feats, out_feats)
        # Manually add bias Bias
        self.r_bias = nn.Parameter(torch.rand(out_feats))
        self.u_bias = nn.Parameter(torch.rand(out_feats))
        self.c_bias = nn.Parameter(torch.rand(out_feats))

    def forward(self, g, x, h):
        # reset gate: how much of the past information to forget
        r = torch.sigmoid(self.r_net(
            g, torch.cat([x, h], dim=1)) + self.r_bias)
        # update gate: how much of the past information needs to be passed along to the future
        u = torch.sigmoid(self.u_net(
            g, torch.cat([x, h], dim=1)) + self.u_bias)
        # current memory content: store relevant information from the past
        h_ = r*h
        c = torch.tanh(self.c_net(
            g, torch.cat([x, h_], dim=1)) + self.c_bias)
        # final memory at current step
        new_h = u*h + (1-u)*c
        return new_h
