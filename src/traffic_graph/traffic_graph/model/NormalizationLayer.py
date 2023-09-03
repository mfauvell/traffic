import torch.nn as nn

class NormalizationLayer(nn.Module):
    def __init__(self, minimum, maximum):
        self.min = minimum
        self.max = maximum
        self.range = self.max - self.min
        self.range[self.range == 0] = 1

    # Here we shall expect mean and std be scaler
    def normalize(self, x):
        return (x - self.min) / self.range

    def denormalize(self, x):
        return x*self.range[0] + self.min[0]