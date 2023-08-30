import torch
import numpy as np
from torch.utils.data import Dataset

class SnapShotDataset(Dataset):
    def __init__(self, x, y, n_data_points=None):
        self.x = x
        self.y = y
        if n_data_points is not None:
            self.x = self.x[:n_data_points]
            self.y = self.y[:n_data_points]
        self.min = np.nanmin(self.x, axis=(0, 1, 2))
        self.max = np.nanmax(self.x, axis=(0, 1, 2))

    def __len__(self):
        return len(self.x)

    def __getitem__(self, idx):
        if torch.is_tensor(idx):
            idx = idx.tolist()

        return self.x[idx, ...], self.y[idx, ...]