import os
import pickle
import numpy as np
import traffic_graph.traffic_graph as tg
import pymongo
from matplotlib import pyplot as plt
import matplotlib.cm as cm
from itertools import cycle

basePath = 'results/featuresStudy_first'
mongoClient = pymongo.MongoClient("mongodb://root:root@localhost:27017/")
trafficDb = mongoClient['traffic']
resultCollection = trafficDb['featuresStudyFirst']
resultCollection.drop()
limitFold = 0
figureMaes, axMae = plt.subplots(figsize=(9, 9))
figureSeparatedMaes, axSeparatedMae = plt.subplots(3,3, sharey=True)
indexX = 0
indexY = 0 
cycler = cycle(cm.tab10.colors)
for config in [dirItem for dirItem in os.listdir(basePath) if os.path.isdir(basePath + '/' + dirItem)]:
    resultArray = []
    names = []
    valuesMaes = []
    internalPath = basePath + '/' + config
    for fold in [dirFold for dirFold in os.listdir(internalPath) if os.path.isdir(internalPath + '/' + dirFold)]:
        if (int(fold[-1]) < limitFold):
            continue
        foldPath = internalPath + '/' + fold
        maesTrain = None
        msesTrain = None
        maesTest = None
        msesTest = None
        with open(f"{foldPath}/maes_train.pkl", "rb") as f:
            maesTrain = pickle.load(f)
        with open(f"{foldPath}/mses_train.pkl", "rb") as f:
            msesTrain = pickle.load(f)
        with open(f"{foldPath}/maes_test.pkl", "rb") as f:
            maesTest = pickle.load(f)
        with open(f"{foldPath}/mses_test.pkl", "rb") as f:
            msesTest = pickle.load(f)
        bestEpoch = 0
        bestTrainMaes = maesTrain[0]
        bestTrainMses = msesTrain[0]
        bestTestMaes = maesTest[0] # Really is the same index of Train
        bestTestMses = msesTest[0]
        for index, data in enumerate(maesTrain):
            if (bestTrainMaes > data):
                bestEpoch = index
                bestTrainMaes = maesTrain[index]
                bestTrainMses = msesTrain[index]
                bestTestMaes = maesTest[index]
                bestTestMses = msesTest[index]
        meanTrainMaes = np.mean(maesTrain)
        meanTrainMses = np.mean(msesTrain)
        meanTestMaes = np.mean(maesTest)
        meanTestMses = np.mean(msesTest)
        varTrainMaes = np.var(maesTrain)
        varTrainMses = np.var(msesTrain)
        varTestMaes = np.var(maesTest)
        varTestMses = np.var(msesTest)
        names.append(fold[-1])
        valuesMaes.append(bestTrainMaes)
        resultArray.append(
            dict(
                config = config,
                fold = fold,
                dateTrain = tg.data_prepare.get_training_date_from_fold(int(fold[-1])),
                bestEpoch = bestEpoch,
                bestTrainMaes = bestTrainMaes,
                bestTrainMses = bestTrainMses,
                bestTestMaes = bestTestMaes,
                bestTestMses = bestTestMses,
                meanTrainMaes = meanTrainMaes,
                meanTrainMses = meanTrainMses,
                meanTestMaes = meanTestMaes,
                meanTestMses = meanTestMses,
                varTrainMaes = varTrainMaes,
                varTrainMses = varTrainMses,
                varTestMaes = varTestMaes,
                varTestMses = varTestMses
            )
        )
    color = next(cycler)
    axSeparatedMae[indexX, indexY].plot(names,valuesMaes, color=color)
    axSeparatedMae[indexX, indexY].set_title('Config: ' + config[-1])
    axMae.plot(names,valuesMaes, label="Config"+config[-1])
    indexY = indexY + 1
    if indexY == 3:
        indexY = 0
        indexX = indexX + 1
        if indexX == 3:
            indexX = 0
    # print(resultArray)
    if (len(resultArray) > 0):
        resultCollection.insert_many(resultArray)
for ax in axSeparatedMae.flat:
    ax.set(xlabel='fold')

# Hide x labels and tick labels for top plots and y ticks for right plots.
for ax in axSeparatedMae.flat:
    ax.label_outer()
figureSeparatedMaes.tight_layout(pad=1)
figureSeparatedMaes.savefig(f"{basePath}/maes_separated_config.svg")
figureMaes.tight_layout()
figureMaes.legend(loc='upper center')
figureMaes.savefig(f"{basePath}/maes_config.svg")
plt.close(figureMaes)
plt.close(figureSeparatedMaes)