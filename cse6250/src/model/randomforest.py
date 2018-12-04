import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn import metrics
from sklearn.model_selection import train_test_split
from imblearn.over_sampling import RandomOverSampler
from imblearn.combine import SMOTEENN
from imblearn.combine import SMOTETomek
from scipy.sparse import dok_matrix


# Merge features with patient information (age, gender) as well as
# any previous probabilities.  Set any missing previous probability
# values to -1 (will be skipped in sparse matrix).
def merge_patients(df, p, prev_prob = None):
    print('merging patients...')
    combined = df.join(p.set_index(['patientID', 'hadmID']), on=['patientID', 'hadmID'])
    if prev_prob is not None:
        combined = combined.join(prev_prob.set_index(['patientID', 'hadmID']), on=['patientID', 'hadmID'])
        combined.loc[combined.prev_prob.isna(), 'prev_prob'] = -1.0
    else:
        combined['prev_prob'] = -1
    combined.to_csv('merged.csv', index=False)
    return combined

# Select a group of test patients to use throughout the entire
# training process.
def get_test_ids():
    df = pd.read_csv('features504.csv')
    print('Collecting test set from {} rows'.format(len(df)))
    septic = df[df.isSeptic==1]
    nonseptic = df[df.isSeptic==0]
    test = septic.sample(frac=0.3, replace=False)
    test.append(nonseptic.sample(n=len(test), replace=False))
    return test.patientID


# Use everyone except the test group for training.
def split(X, ids):
    return (X[~X.patientID.isin(ids)],
	    X[X.patientID.isin(ids)])


# Create a sparse matrix from the features.
# Treat -1 as missing.
def to_sparse(X):
    n, c = X.shape
    print(n, c)
    sm = dok_matrix((n, c), dtype=np.float32)
    for (i,row) in enumerate(X):
        for j in range(c):
            if row[j] > -1.0:
                sm[i,j] = row[j]
    return sm


# Build a model for a specific aggregation window.
def model_a_file(n, p, test_ids, prev_prob=None):
    filnam = "features"+str(n)+".csv"

    df_train, df_test  = split(merge_patients(pd.read_csv(filnam), p, prev_prob), ids)
    
    print('running model...')
    x_train = df_train[ ['minSystolicBP', 'maxSystolicBP', 'minHeartRate', 'maxHeartRate', 'minRespRate', 'maxRespRate', 'minTempC', 'maxTempC', 'minSpO2', 'minPulsePressure', 'maxPulsePressure', 'minGCS', 'age', 'gender', 'prev_prob' ] ]
    y_train = df_train[ 'isSeptic' ]
    x_train = to_sparse(x_train.values)

    x_test = df_test[ ['minSystolicBP', 'maxSystolicBP', 'minHeartRate', 'maxHeartRate', 'minRespRate', 'maxRespRate', 'minTempC', 'maxTempC', 'minSpO2', 'minPulsePressure', 'maxPulsePressure', 'minGCS', 'age', 'gender', 'prev_prob' ] ]
    x_test = to_sparse(x_test.values)

    y_test = df_test[ 'isSeptic' ]

    # Use naive oversampling.
    ros = RandomOverSampler()

    clf = RandomForestClassifier(n_estimators=100, max_depth=6)

    x_train, y_train = ros.fit_resample(x_train, y_train)

    clf.fit(x_train,  y_train)

    df_y_pred = clf.predict(x_test)
    df_y_prob = clf.predict_proba(x_test)[:,1]
    fpr, tpr, thresholds = metrics.roc_curve(y_test, df_y_prob, pos_label=1)
    auc = metrics.auc(fpr, tpr)
    print("~~~~ Modelling file "+ filnam)
    print("~~~~ ~~~~ Confusion matrix")
    print(metrics.confusion_matrix(y_test, df_y_pred))
    print("~~~~ ~~~~ AUC: {}".format(auc))
    df_test['prev_prob'] = df_y_prob
    return df_test[['patientID', 'hadmID', 'prev_prob']]

print("Modelling the feature files")
p = pd.read_csv('PATIENTDETAILS.csv')
ids = get_test_ids()
prev_df_test = None
for i in range(42):
    prev_df_test = model_a_file(12*(1+i), p, ids, prev_df_test)
