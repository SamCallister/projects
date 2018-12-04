from sklearn.datasets import dump_svmlight_file
from sklearn.model_selection import train_test_split
import pandas as pd
import argparse


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--test',
                   type=float,
                   default=0.3,
                   help='specify the test fraction')
    p.add_argument('-d',
                   '--data',
                   help='specify the feature file')
    opts = p.parse_args()
    sf = pd.read_csv(opts.data)
    train, test = train_test_split(sf, test_size=opts.test)

    train_x = train.drop(columns=['patientID', 'hadmID', 'isSeptic'])
    train_y = train['isSeptic']

    test_x = test.drop(columns=['patientID', 'hadmID', 'isSeptic'])
    test_y = test['isSeptic']

    with open('train.svmlight', 'wb') as fp:
        dump_svmlight_file(train_x, train_y, fp)

    with open('test.svmlight', 'wb') as fp:
        dump_svmlight_file(test_x, test_y, fp)


if __name__ == '__main__':
    main()
