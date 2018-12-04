import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn import metrics
from sklearn.ensemble import AdaBoostClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.neural_network import MLPClassifier
import random

##########

def model_a_file(n):
    filnam = "sofafeatures.hour."+str(n)+".csv"

    df = pd.read_csv(filnam)

    X_column_names = list(set(list(df.columns)) - set(['patientID', 'hadmID','isSeptic']))
    df_X = df[ X_column_names ]

    df_y = df[ 'isSeptic' ]

    scaler = StandardScaler()

    clf = RandomForestClassifier(n_estimators=100
    #max_features=4,
    #class_weight="balanced_subsample"
    #criterion="entropy"
    )

    x_train, x_test, y_train, y_test = train_test_split(scaler.fit_transform(df_X), df_y, test_size=0.2)

    clf.fit(x_train,  y_train)

    df_y_pred = clf.predict(x_test)
    df_y_prob = clf.predict_proba(x_test)[:,1]
    fpr, tpr, thresholds = metrics.roc_curve(y_test, df_y_prob, pos_label=1)
    auc = metrics.auc(fpr, tpr)
    print("~~~~ Modelling file "+ filnam)
    print("~~~~ ~~~~ Confusion matrix")
    print(metrics.confusion_matrix(y_test, df_y_pred))
    print("~~~~ ~~~~ AUC: {}".format(auc))

print("Modelling the feature files")
for i in range(29):
    model_a_file(12*(1+i))

