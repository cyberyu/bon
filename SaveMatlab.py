__author__ = 'shyu'

import pickle
from scipy import sparse, io

model_train_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\total_train_diag_proc_only.pkl'

mat_train = open(model_train_file,'rb')

model_data = pickle.load(mat_train)

io.savemat('D:\\BonSecours\\3weeks\\db_dump\\new_data\\dump.mat', dict(m=model_data))







