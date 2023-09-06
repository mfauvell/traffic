import traffic_graph.traffic_graph as tg
from torch.utils.data import DataLoader
import time
from functools import partial
import dgl
import torch
import torch.nn as nn
import numpy as np
from matplotlib import pyplot as plt
import pickle
batch_cnt = [0]

def make_train(arrx, arry, graph, time_gaps, dates, train_time, config, path):
    print('Starting Train')
    print('Train until: ' + train_time)
    device = get_device(config)
    arrxTrain, arryTrain, arrxTest, arryTest = tg.data_prepare.get_train_test_arrays(arrx, arry, time_gaps, dates, train_time, config)
    trainDataset = tg.model.SnapShotDataset(arrxTrain, arryTrain)
    testDataset = tg.model.SnapShotDataset(arrxTest, arryTest)
    ## save datasets
    #TODO:
    trainLoader = DataLoader(trainDataset, batch_size=config['batch_size'], num_workers=config['num_workers'], shuffle=True)
    testLoader = DataLoader(testDataset, batch_size=config['batch_size'], num_workers=config['num_workers'], shuffle=True)
    print("Shape of train_x:", trainDataset.x.shape)
    print("Shape of train_y:", trainDataset.y.shape)
    print("Shape of test_x:", testDataset.x.shape)
    print("Shape of test_y:", testDataset.y.shape)
    seq_len = trainDataset.x.shape[1]
    in_feats = trainDataset.x.shape[-1]
    normalizer = tg.model.NormalizationLayer(trainDataset.min, trainDataset.max)
    batch_g = dgl.batch([graph] * config['batch_size']).to(device)
    out_gs, in_gs = tg.model.DiffConv.attach_graph(batch_g, config['diffsteps'])
    net = partial(tg.model.DiffConv, k=config['diffsteps'], in_graph_list=in_gs, out_graph_list=out_gs, dir=config["direction"])
    dcrnn = tg.model.GraphRNN(in_feats=in_feats,
        out_feats=config['out_feats'],
        seq_len=seq_len,
        num_layers=config['num_layers'],
        net=net,
        decay_steps=config['decay_steps']).to(device)
    reset_parameters(dcrnn)
    optimizer = torch.optim.Adam(dcrnn.parameters(), lr=config['lr'])
    scheduler = torch.optim.lr_scheduler.ExponentialLR(optimizer, gamma=0.99)
    loss_fn = masked_mae_loss
    train_maes = []
    train_mses = []
    test_maes = []
    test_mses = []
    for e in range(config['epochs']):
        train(dcrnn, graph, trainLoader, optimizer, scheduler, normalizer, loss_fn, device, config['batch_size'], config['max_grad_norm'], config['minimum_lr'])
        train_mae, train_mse = eval(dcrnn, graph, trainLoader, normalizer, loss_fn, device, config['batch_size'])
        test_mae, test_mse = eval(dcrnn, graph, testLoader, normalizer, loss_fn, device, config['batch_size'])
        print(f"Epoch: {e} Train MAE: {train_mae} Train MSE: {train_mse} Test MAE: {test_mae} Test MSE: {test_mse}")

        train_maes.append(train_mae)
        train_mses.append(train_mse)
        test_maes.append(test_mae)
        test_mses.append(test_mse)

        fig, ax = plt.subplots(figsize=(14, 4))
        ax.plot(train_maes, label="train")
        ax.plot(test_maes, label="test")
        plt.legend()
        plt.savefig(f"{path}/learning_curve_mae.svg")
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(14, 4))
        ax.plot(train_mses, label="train")
        ax.plot(test_mses, label="test")
        plt.legend()
        plt.savefig(f"{path}/learning_curve_mse.svg")
        plt.close(fig)

        ### save model
        torch.save(dcrnn.state_dict(), f"{path}/model{e}.pt")

        if len(train_maes) >= 3:
            if all([train_mae > previous_mae for previous_mae in train_maes[-3:-1]]):
                break
            if all([train_mse > previous_mse for previous_mse in train_mses[-3:-1]]):
                break

    del optimizer
    del scheduler
    del net
    del out_gs
    del in_gs
    print("Training finished")
    # save mae and mes
    with open(f"{path}/loss_train.pkl", "wb") as f:
        pickle.dump(train_maes, f)
    with open(f"{path}/loss_test.pkl", "wb") as f:
        pickle.dump(test_mses, f)

    return dcrnn
    
def masked_mae_loss(y_pred, y_true):
    mask = (y_true != 0).float()
    mask /= mask.mean()
    loss = torch.abs(y_pred - y_true)
    loss = loss * mask
    # trick for nans: https://discuss.pytorch.org/t/how-to-set-nan-in-tensor-to-0/3918/3
    loss[loss != loss] = 0
    return loss.mean()

def train(model, graph, dataloader, optimizer, scheduler, normalizer, loss_fn, device, batch_size, max_grad_norm, minimum_lr):
    mae_loss = []
    mse_loss = []
    graph = graph.to(device)
    model.train()
    for i, (x, y) in enumerate(dataloader):
        optimizer.zero_grad()
        y, y_pred = predict(x, y, batch_size, graph, model, device, normalizer)
        mae = torch.nn.L1Loss()(y_pred, y)
        mse = torch.nn.MSELoss()(y_pred, y)
        mae.backward()
        nn.utils.clip_grad_norm_(model.parameters(), max_grad_norm)
        optimizer.step()
        if get_learning_rate(optimizer) > minimum_lr:
            scheduler.step()
        mae_loss.append(float(mae))
        mse_loss.append(float(mse))
        batch_cnt[0] += 1
        print("Batch: ", i, end="\r")
    return np.mean(mae_loss), np.mean(mse_loss)

def eval(model, graph, dataloader, normalizer, loss_fn, device, batch_size):
    mae_loss = []
    mse_loss = []
    graph = graph.to(device)
    model.eval()
    batch_size = batch_size
    for i, (x, y) in enumerate(dataloader):
        y, y_pred = predict(x, y, batch_size, graph, model, device, normalizer)
        mae = torch.nn.L1Loss()(y_pred, y)
        mse = torch.nn.MSELoss()(y_pred, y)
        mae_loss.append(float(mae))
        mse_loss.append(float(mse))
    return np.mean(mae_loss), np.mean(mse_loss)

def predict(x, y, batch_size, graph, model, device, normalizer):
    x, y, x_norm, y_norm, batch_graph = prepare_data(x, y, batch_size, graph, normalizer, device)
    output = model(batch_graph, x_norm, y_norm, batch_cnt[0], device)
    output = output[:, :, [0]]
    # Denormalization for loss compute
    y_pred = normalizer.denormalize(output)
    return y, y_pred

def prepare_data(x, y, batch_size, graph, normalizer, device):
    x, y = padding(x, y, batch_size)
    # Permute the dimension for shaping
    x = x.permute(1, 0, 2, 3)
    y = y.permute(1, 0, 2, 3)
    x_norm = normalizer.normalize(x).reshape(
        x.shape[0], -1, x.shape[3]).float().to(device)
    y_norm = normalizer.normalize(y).reshape(
        y.shape[0], -1, y.shape[3]).float().to(device)
    y = y.reshape(y.shape[0], -1, y.shape[3]).to(device)
    batch_graph = dgl.batch([graph] * batch_size)
    y = y[:, :, [0]]
    return x, y, x_norm, y_norm, batch_graph

def padding(x, y, batch_size):
    # Padding: Since the diffusion graph is precomputed we need to pad the batch so that
    # each batch have same batch size
    if x.shape[0] != batch_size:
        x_buff = torch.zeros(
            batch_size, x.shape[1], x.shape[2], x.shape[3])
        y_buff = torch.zeros(
            batch_size, x.shape[1], x.shape[2], x.shape[3])
        x_buff[:x.shape[0], :, :, :] = x
        x_buff[x.shape[0]:, :, :, :] = x[-1].repeat(batch_size - x.shape[0], 1, 1, 1)
        y_buff[:x.shape[0], :, :, :] = y
        y_buff[x.shape[0]:, :, :, :] = y[-1].repeat(batch_size - x.shape[0], 1, 1, 1)
        x = x_buff
        y = y_buff
    return x, y

def get_learning_rate(optimizer):
    for param in optimizer.param_groups:
        return param['lr']
    
def get_device(config):
    if torch.cuda.is_available() and config['gpu'] == 1:
        print('Using Cuda')
        device = torch.device('cuda:0')
    else:
        print('Using CPU')
        device = torch.device('cpu')
    return device

def reset_parameters(model):
    for layer in model.children():
        if hasattr(layer, 'reset_parameters'):
            print('holy shit')
            layer.reset_parameters()