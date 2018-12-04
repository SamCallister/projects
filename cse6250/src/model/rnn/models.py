import torch
import torch.nn as nn
from torch.nn.utils.rnn import pack_padded_sequence, pad_packed_sequence
import torch.nn.functional as F

class VariableRNN(nn.Module):
	def __init__(self, dimInput):
		super(VariableRNN, self).__init__()
		self.input = nn.Linear(dimInput, 8)
		self.rnn = nn.GRU(input_size=8, hidden_size=6, num_layers=2,batch_first=True)
		# self.hiddenLayer1 = nn.Linear(9, 7)
		# self.hiddenLayer2 = nn.Linear(7, 5)
		# self.hiddenLayer3 = nn.Linear(5, 4)
		self.outputLayer = nn.Linear(6, 2)

	def forward(self, input_tuple):
		# HINT: Following two methods might be useful
		# 'pack_padded_sequence' and 'pad_packed_sequence' from torch.nn.utils.rnn
		seqs, lengths = input_tuple
		x = torch.tanh(self.input(seqs))
		x = pack_padded_sequence(seqs, lengths=lengths, batch_first=True)
		x, _ = self.rnn(x)

		# weight_list = []
		# for name in self.rnn.named_parameters():
		# 	if 'weight' in name[0]:
		# 		weight_list.append(name[1])
		# print(weight_list)
		# print(sum(lengths))
		# print(x.data.shape)
		# print(x.data)

		x, tensorLengths = pad_packed_sequence(x, batch_first=True)
		tensors = []
		for i, b in enumerate(x):
			l = lengths[i] - 1
			tensors.append(b[l, :])
		x = torch.stack(tensors)
	
		x = torch.tanh(x)
		# x = torch.tanh(self.hiddenLayer1(x))
		# x = F.relu(self.hiddenLayer2(x))
		# x = F.relu(self.hiddenLayer3(x))
		x = self.outputLayer(x)

		return x