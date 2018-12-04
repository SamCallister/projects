import pandas as pd
import argparse
import sys
import numpy as np


MILLIS_PER_HOUR = 1000*60*60

FEATURE_COLS = ['patientID',
                'hadmID',
                'chartTime',
                'minRespRatio',
                'minPlatelets',
                'maxBilirubin',
                'minMeanBP',
                'maxNorepinephrine',
                'maxEpinephrine',
                'maxDopamine',
                'maxDobutamine',
                'minGCS',
                'maxCreatinine',
                'netUrine']

JOINED_COLS = FEATURE_COLS + ['windowStart']


NEW_COLS = ['patientID',
            'hadmID']
for col in FEATURE_COLS[3:]:
    NEW_COLS.append(col)
    NEW_COLS.append('{}Missing'.format(col))

            
def load_data(opts):
    features = pd.read_csv(opts.features).sort_values(by=['chartTime'])
    windows = pd.read_csv(opts.window)
    labels = pd.read_csv(opts.labels)
    labels = labels[['patientID', 'hadmID', 'onsetDelta']]
    features = features.join(labels.set_index(['patientID', 'hadmID']),
                             on=['patientID', 'hadmID'])
    features = features.dropna()
    return features, windows, labels


def write_row(row, fp):
    new_row = []
    for col in row.keys():
        new_row.append('{:.2f}'.format(row[col]))
        if (row[col] < 0):
            new_row.append('1')
        else:
            new_row.append('0')
    fp.write(','.join(new_row))


def get_windowed_data(opts):
    features, windows, labels = load_data(opts)
    windows = windows.drop(columns=['windowEnd'])
    joined = features.join(windows.set_index(['patientID', 'hadmID']),
                           on=['patientID', 'hadmID']).dropna()
    return joined, labels


def get_hourly_lists(grouped):
    aggregators = {col: list for col in JOINED_COLS[2:]}
    return grouped.agg(aggregators).reset_index()


def get_hour_data(tup):
    data = []
    nMissing = 0
    for val in tup[1:]:
        data.append('{:.2f}'.format(val))
        if val < 0:
            missing = 1
            nMissing += 1
        else:
            missing = 0
        data.append(missing)
    allMissing = (nMissing == len(tup[1:]))
    return allMissing, data


def write_hourly_data(patientID, hadmID, data, fp):
    allMissing = np.all([pair[0] for pair in data])
    if not allMissing:
        fp.write('{},{},'.format(patientID, hadmID))
        fp.write('{},'.format(','.join([','.join([str(val) for val in pair[1]]) for pair in data])))
        return False
    return True


def write_hourly_window(firstHour, windowSize, hourly, labels, fp):
    hourLabels = ((labels.onsetDelta >= 0) & (labels.onsetDelta <= (firstHour + windowSize))).apply(int)
    isSeptic = {(pid, hadmID): l for (pid, hadmID, l) in zip(labels.patientID, labels.hadmID, hourLabels)}
    fp.write('{},{},'.format(FEATURE_COLS[0],
                             FEATURE_COLS[1]))
    for i in range(windowSize-1):
        for f in FEATURE_COLS[3:]:
            fp.write('{}Hour{},{}Hour{}Missing,'.format(f, firstHour + i + 1, f, firstHour + i + 1))
    fp.write(','.join(['{}Hour{},{}Hour{}Missing'.format(f, firstHour + windowSize, f, firstHour + windowSize)
                       for f in FEATURE_COLS[3:]]))
    fp.write(',isSeptic\n')
    for row in hourly.iterrows():
        items = row[1]
        patientID = items['patientID']
        hadmID = items['hadmID']
        windowStart = items['windowStart'][0] + firstHour * MILLIS_PER_HOUR
        onsetDelta = labels[(labels.patientID == patientID) & (labels.hadmID == hadmID)]['onsetDelta'].head(1).values[0]
        if onsetDelta < firstHour:
            continue
        maxWindow = windowStart + (windowSize-1) * MILLIS_PER_HOUR
        zipped = list(zip(*items[FEATURE_COLS[2:]]))
        terminated = False
        hoursToGet = [(True, ["-1.0,1" for col in FEATURE_COLS[3:]]) for i in range(windowSize)]
        for tup in zipped:
            if tup[0] < windowStart:
                continue
            if tup[0] > maxWindow:
                terminated = True
                break
            idx = int((tup[0] - windowStart) / MILLIS_PER_HOUR)
            hoursToGet[idx] = get_hour_data(tup)
        allMissing = write_hourly_data(patientID,
                                       hadmID,
                                       hoursToGet,
                                       fp)
        if not allMissing:
            fp.write(str(isSeptic[(patientID, hadmID)]))
            fp.write('\n')

def main():
    p = argparse.ArgumentParser()
    p.add_argument('-f',
                   '--features',
                   default='sofafeatures.with.header.csv')
    p.add_argument('-w', 
                   '--window',
                   default='SOFAWINDOW.csv')
    p.add_argument('-l', 
                   '--labels',
                   default='PATIENTLABELS.csv')
    opts = p.parse_args()
    data, labels = get_windowed_data(opts)
    hourly = get_hourly_lists(data.groupby(['patientID', 'hadmID']))
    for firstHour in range(0, 12*42, 12):
        lastHour = firstHour + 12
        print('Writing up to hour {}...'.format(lastHour))
        fpath = 'sofafeatures.hour.{}.csv'.format(lastHour)
        with open(fpath, 'w') as fp:
            write_hourly_window(firstHour, 12, hourly, labels, fp)


if __name__ == '__main__':
    main()

    
