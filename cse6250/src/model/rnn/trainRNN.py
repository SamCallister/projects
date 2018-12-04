import pickle
import numpy as np
from customDataLoading import VisitSequenceWithLabelDataset, visitCollateFn
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader
from models import VariableRNN
from utils import train, evaluate
from confusionMatrix import plotConfusionMatrix
from sklearn.metrics import roc_auc_score

NUM_WORKERS = 4
NUM_EPOCHS = 4
BATCH_SIZE = 100
USE_CUDA = True

# load up dataset
dataTrain = np.load('./output/dataTrain.npy')
targetTrain = np.load('./output/targetTrain.npy')

dataValidate = np.load('./output/dataValidate.npy')
targetValidate = np.load('./output/targetValidate.npy')

dataTest = np.load('./output/dataTest.npy')
targetTest = np.load('./output/targetTest.npy')


numFeatures = len(dataTrain[0][0])

trainDataset = VisitSequenceWithLabelDataset(dataTrain, targetTrain)
validDataset = VisitSequenceWithLabelDataset(dataValidate, targetValidate)
testDataset = VisitSequenceWithLabelDataset(dataTest, targetTest)

trainLoader = DataLoader(dataset=trainDataset, batch_size=BATCH_SIZE, shuffle=True, collate_fn=visitCollateFn, num_workers=NUM_WORKERS)
validLoader = DataLoader(dataset=validDataset, batch_size=BATCH_SIZE, shuffle=False, collate_fn=visitCollateFn, num_workers=NUM_WORKERS)
testLoader = DataLoader(dataset=testDataset, batch_size=1, shuffle=False, collate_fn=visitCollateFn, num_workers=NUM_WORKERS)

model = VariableRNN(numFeatures)
criterion = nn.CrossEntropyLoss()
#ASGD
optimizer = optim.Adam(model.parameters(), lr=0.1)
device = torch.device("cuda" if torch.cuda.is_available() and USE_CUDA else "cpu")
model.to(device)
criterion.to(device)

best_val_acc = 0.0
train_losses, train_accuracies = [], []
valid_losses, valid_accuracies = [], []
for epoch in range(NUM_EPOCHS):
    train_loss, train_accuracy = train(model, device, trainLoader, criterion, optimizer, epoch)
    valid_loss, valid_accuracy, valid_results = evaluate(model, device, validLoader, criterion)

    # for x in valid_results:
    #     print('%i : %i' % (x[0], x[1]))

    train_losses.append(train_loss)
    train_accuracies.append(train_accuracy)
    valid_losses.append(valid_loss)
    valid_accuracies.append(valid_accuracy)

    is_best = valid_accuracy > best_val_acc  # let's keep the model that has the best accuracy, but you can also use another metric.
    if is_best:
        best_val_acc = valid_accuracy
        torch.save(model, 'output/bestRNN.pth')

best_model = torch.load('output/bestRNN.pth')

# TODO: For your report, try to make plots similar to those in the previous task.
# TODO: You may use the validation set in case you cannot use the test set.
print('best validation accuracy model')
test_loss, test_accuracy, test_results = evaluate(best_model, device, testLoader, criterion)
actual = [x[0] for x in test_results]
pred = [x[1] for x in test_results]
plotConfusionMatrix(actual, pred, ['not septic', 'septic'], 'bestValidationModelConfusion1.png')
aucScore = roc_auc_score(actual, pred)
print(actual)
print(pred)
print('final accuracy')
print(test_accuracy)
print('final auc')
print(aucScore)


print('final model')
test_loss, test_accuracy, test_results = evaluate(model, device, testLoader, criterion)
actual = [x[0] for x in test_results]
pred = [x[1] for x in test_results]
plotConfusionMatrix(actual, pred, ['not septic', 'septic'], 'finalModelConfusion1.png')
aucScore = roc_auc_score(actual, pred)
print(actual)
print(pred)
print('final accuracy')
print(test_accuracy)
print('final auc')
print(aucScore)