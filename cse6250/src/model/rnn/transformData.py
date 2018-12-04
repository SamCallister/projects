import numpy as np
import pandas as pd
import math
from sklearn.preprocessing import MinMaxScaler
import torch
import pickle

featureCols = ['minSystolicBP', 'maxSystolicBP', 'minPulsePressure',
                  'maxPulsePressure', 'minHeartRate', 'maxHeartRate',
                 'minRespRate', 'maxRespRate', 'minTempC', 'maxTempC',
                  'minSpO2', 'minGCS', 'hourDelta']
idCols = ['patientID', 'hadmID']
colsNotToScale = ['patientID', 'hadmID', 'chartTime', 'hourDelta', 'isSeptic']
labelCol = 'isSeptic'

"""
Pads sequence up to max length
@Returns paddedSeq
"""
def padSeq(v, maxLength, numFeatures):
    howManyNewPoints = maxLength - len(v)
    toAdd = np.zeros((howManyNewPoints, numFeatures))
    return np.append(v, toAdd, axis=0)

"""
dataTensor is of the form [ [[f1, f2,...], [f1, f2,...]], ...]
"""
def transformIntoDataTarget(df):
    groups = df.groupby(idCols).groups
    indexesForEachPatient = list(groups.values())

    examples = []
    labels = []

    # get the features extracted and the label
    for i in indexesForEachPatient:
        sortedDf = df.loc[i].sort_values(by='hourDelta')
        rowsForPatient = sortedDf[featureCols].values
        examples.append(rowsForPatient)
        labels.append(sortedDf[labelCol].iloc[0])

    # pad the features
    # lengths = np.array([len(l) for l in examples])
    # maxLength = max(lengths)
    # numFeatures = len(examples[0][0])
    # paddedData = [padSeq(v, maxLength, numFeatures) for v in examples]
    # tensorData = np.array(paddedData)
    target = np.array(labels)

    return (examples, target)

def scaleData(df):
    scaler = MinMaxScaler()
    return pd.DataFrame(scaler.fit_transform(df), columns=df.columns)

"""
Scales data and splits into training and testing sets. 
@returns (trainDf, testDf)
"""
def splitIntoTrainAndTestDf(df, testFrac=0.2):
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

    return (trainFrame, testFrame)

def loadAndScaleData(path):
    df = pd.read_csv(path)
    colsToScale = [c for c in df.columns if c not in set(colsNotToScale)]
    df = df[colsNotToScale].join(scaleData(df[colsToScale]))

    return df

def limitNumberFromEachClass(df, lim):
    groups = df.groupby(idCols + ['isSeptic']).groups

    totalSeptic = 0
    totalNonSeptic = 0
    indexesToKeep = np.array([])
    for key, value in groups.items():
        isSeptic = key[2]

        if isSeptic == 1 and totalSeptic < lim:
            totalSeptic += 1
            indexesToKeep = np.append(indexesToKeep, value)
        elif isSeptic == 0 and totalNonSeptic < lim:
            totalNonSeptic += 1
            indexesToKeep = np.append(indexesToKeep, value)

    return df.loc[indexesToKeep]

if __name__ == "__main__":
    PATH_TO_DATA = '~/data/FEATURES.csv'
    df = loadAndScaleData(PATH_TO_DATA)

    trainDf, testDf = splitIntoTrainAndTestDf(df)
    trainSet, validationSet = splitIntoTrainAndTestDf(trainDf)
    # trainSet = limitNumberFromEachClass(trainSet, 3000)
    # validationSet = limitNumberFromEachClass(validationSet, 1500)

    data, target = transformIntoDataTarget(trainSet)
    np.save('./output/dataTrain', data)
    np.save('./output/targetTrain', target)

    data, target = transformIntoDataTarget(validationSet)
    np.save('./output/dataValidate', data)
    np.save('./output/targetValidate', target)

    data, target = transformIntoDataTarget(testDf)
    np.save('./output/dataTest', data)
    np.save('./output/targetTest', target)