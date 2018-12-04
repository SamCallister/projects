from sklearn.metrics import confusion_matrix
import matplotlib.pyplot as plt
from itertools import product
import numpy as np

# results is yTrue, yPred
def plotConfusionMatrix(actualRes, predictedRes, class_names, filename):

	# Confusion matrix example code taken from http://scikit-learn.org/stable/auto_examples/model_selection/plot_confusion_matrix.html#sphx-glr-auto-examples-model-selection-plot-confusion-matrix-py
	cm = confusion_matrix(actualRes, predictedRes)

	cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]

	plt.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
	plt.title('Normalized Confusion Matrix')
	plt.colorbar()
	tick_marks = np.arange(len(class_names))
	plt.xticks(tick_marks, class_names, rotation=45)
	plt.yticks(tick_marks, class_names)
	thresh = 0.5

	for i, j in product(range(cm.shape[0]), range(cm.shape[1])):
		plt.text(j, i, format(cm[i, j], '.2f'),
				 horizontalalignment="center",
				 color="white" if cm[i, j] > thresh else "black")

	plt.ylabel('True')
	plt.xlabel('Predicted')
	plt.tight_layout()
	# end referenced example

	plt.savefig(filename)
	plt.clf()