import pandas as pd
import numpy as np
import math
from sklearn.preprocessing import MinMaxScaler

featureColumns = ['minSystolicBP', 'maxSystolicBP', 'minPulsePressure',
                  'maxPulsePressure', 'minHeartRate', 'maxHeartRate',
                 'minRespRate', 'maxRespRate', 'minTempC', 'maxTempC',
                  'minSpO2', 'minGCS', 'hourDelta']
idCols = ['patientID', 'hadmID']
labelCol = 'isSeptic'

def scaleData(df):
    scaler = MinMaxScaler()
    return pd.DataFrame(scaler.fit_transform(df), columns=df.columns)

def getSplitTrainAndTest(path, testFrac=0.2):
    PATH_TO_DATA = '~/data/FEATURES.csv'
    df = pd.read_csv(PATH_TO_DATA)

    groups = df.groupby(idCols).groups
    numGroups = len(groups)
    nTest = math.floor(numGroups * testFrac)

    testPatients = np.random.choice(numGroups, nTest)
    pIndexes = list(groups)
    testPatientIndexes = pd.Int64Index([])
    for i in testPatients:
        p = pIndexes[i]
        testPatientIndexes = testPatientIndexes.union(groups[p])

    testFrame = df.loc[df.index.isin(testPatientIndexes)]
    trainFrame = df.loc[~df.index.isin(testPatientIndexes)]

    trainX = scaleData(trainFrame[featureColumns])
    trainY  = trainFrame[labelCol]
    testX = scaleData(testFrame[featureColumns])
    testY  = testFrame[labelCol]

    return (trainX, trainY, testX, testY)
