from scipy import sparse
from torch.utils.data import Dataset
import numpy as np
import torch

class VisitSequenceWithLabelDataset(Dataset):
	def __init__(self, seqs, labels):
		"""
		Args:
			seqs (list): list of patients (list) of visits (list) of codes (int) that contains visit sequences
			labels (list): list of labels (int)
			num_features (int): number of total features available
		"""

		if len(seqs) != len(labels):
			raise ValueError("Seqs and Labels have different lengths")

		self.labels = labels
		self.seqs = seqs


	def __len__(self):
		return len(self.labels)

	def __getitem__(self, index):
		# returns will be wrapped as List of Tensor(s) by DataLoader
		return self.seqs[index], self.labels[index]


def visitCollateFn(batch):
	"""
	DataLoaderIter call - self.collate_fn([self.dataset[i] for i in indices])
	Thus, 'batch' is a list [(seq1, label1), (seq2, label2), ... , (seqN, labelN)]
	where N is minibatch size, seq is a (Sparse)FloatTensor, and label is a LongTensor

	:returns
		seqs (FloatTensor) - 3D of batch_size X max_length X num_features
		lengths (LongTensor) - 1D of batch_size
		labels (LongTensor) - 1D of batch_size
	"""
	batch.sort(key=lambda x: x[0].shape[0], reverse=True)
	maxLengthSeq = max([len(t[0]) for t in batch])
	numFeatures = len(batch[0][0][0])
	
	seqs = []
	lengths = []
	labels = []
	for t in batch:
		seq, label = t

		seqLen = len(seq)
		diffLength = maxLengthSeq - seqLen

		lengths.append(seqLen)
		labels.append(label)

        
		toAdd = np.zeros((diffLength, numFeatures))
		paddedSeq = np.append(seq, toAdd, axis=0)
		seqs.append(paddedSeq)
		
	

	seqs_tensor = torch.FloatTensor(seqs)
	lengths_tensor = torch.LongTensor(lengths)
	labels_tensor = torch.LongTensor(labels)

	return (seqs_tensor, lengths_tensor), labels_tensor