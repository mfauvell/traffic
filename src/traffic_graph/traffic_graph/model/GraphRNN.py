import numpy as np
import torch
import torch.nn as nn
import traffic_graph.traffic_graph as tg

class GraphRNN(nn.Module):
    '''Graph Sequence to sequence prediction framework
    Support multiple backbone GNN. Mainly used for traffic prediction.

    Parameter
    ==========
    in_feats : int
        number of input features

    out_feats : int
        number of prediction output features

    seq_len : int
        input and predicted sequence length

    num_layers : int
        vertical number of layers in encoder and decoder unit

    net : torch.nn.Module
        Message passing GNN as backbone

    decay_steps : int
        number of steps for the teacher forcing probability to decay
    '''

    def __init__(self,
                 in_feats,
                 out_feats,
                 seq_len,
                 num_layers,
                 net,
                 decay_steps):
        super(GraphRNN, self).__init__()
        self.in_feats = in_feats
        self.out_feats = out_feats
        self.seq_len = seq_len
        self.num_layers = num_layers
        self.net = net
        self.decay_steps = decay_steps

        self.encoder = tg.model.StackedEncoder(self.in_feats,
                                      self.out_feats,
                                      self.num_layers,
                                      self.net)

        self.decoder = tg.model.StackedDecoder(self.in_feats,
                                      self.out_feats,
                                      self.in_feats,
                                      self.num_layers,
                                      self.net)
    # Threshold For Teacher Forcing

    def compute_thresh(self, batch_cnt):
        return self.decay_steps/(self.decay_steps + np.exp(batch_cnt / self.decay_steps))

    def encode(self, g, inputs, device):
        hidden_states = [torch.zeros(g.num_nodes(), self.out_feats).to(
            device) for _ in range(self.num_layers)]
        for i in range(self.seq_len):
            _, hidden_states = self.encoder(g, inputs[i], hidden_states)

        return hidden_states

    def decode(self, g, teacher_states, hidden_states, batch_cnt, device):
        outputs = []
        inputs = torch.zeros(g.num_nodes(), self.in_feats).to(device)
        for i in range(self.seq_len):
            if np.random.random() < self.compute_thresh(batch_cnt) and self.training:
                inputs, hidden_states = self.decoder(
                    g, teacher_states[i], hidden_states)
            else:
                inputs, hidden_states = self.decoder(g, inputs, hidden_states)
            outputs.append(inputs)
        outputs = torch.stack(outputs)
        return outputs

    def forward(self, g, inputs, teacher_states, batch_cnt, device):
        hidden = self.encode(g, inputs, device)
        outputs = self.decode(g, teacher_states, hidden, batch_cnt, device)
        return outputs
