__author__ = 'shyu'

import pickle
import sklearn
import scipy
from sklearn.metrics import accuracy_score
from sklearn.ensemble import RandomForestClassifier
from scipy import *
from scipy.sparse import *
import statsmodels.api as sm
import sklearn.datasets as ds
import numpy as np

model_train_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\total_train.pkl'
model_label_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\total_label.pkl'

mat_train = open(model_train_file,'rb')
mat_label = open(model_label_file,'rb')


tot_perf = []
print('The scikit-learn version is {}.'.format(sklearn.__version__))

model_data = pickle.load(mat_train)
label_data = pickle.load(mat_label)

# change to binary case
for loop in range(0,label_data.shape[0]):
    v = int(label_data[loop,0])
    print (v)
    if ((v == 1) | (v==2)):
        label_data[loop,0] = 1
    else:
        label_data[loop,0] = 2

for loop in range(1,11):

    train_ind = np.random.choice(79720,55804, replace=False)
    test_ind = set(range(1,79720))-set(train_ind)

    train_ind = np.asarray(train_ind)
    test_ind = np.asarray(list(test_ind))

    model_train_data = model_data[train_ind,]
    model_test_data = model_data[test_ind,]

    label_train_data = label_data[train_ind,]
    label_test_data = label_data[test_ind,]

    #rf = RandomForestClassifier(n_estimators=100, max_depth=5, min_samples_split=100)   #0.631611959022
    #rf.fit(model_train_data.tocsr(),label_train_data.tocsr().todense())
    #pred = rf.predict(model_test_data.tocsr())

    #X_, y_ = ds.make_classification(n_samples=200, n_features=100,weights=[0.833, 0.167], random_state=0)
    #model_data = csr_matrix(model_data)
    #label_data = label_data[0:100]


    X_train = scipy.sparse.csr_matrix(model_train_data)
    X_test = scipy.sparse.csr_matrix(model_test_data)


    print (label_train_data.get_shape())
    print (label_test_data.get_shape())


    y_train = label_train_data.todense().reshape((label_train_data.shape[0],1))
    y_test = label_test_data.todense().reshape((label_test_data.shape[0],1))

    #sm = sklearn.linear_model.LogisticRegression()  #0.586702906126
    sm = sklearn.linear_model.LogisticRegression(penalty='l2',multi_class='multinomial',solver='lbfgs')

    sm.fit(X_train,y_train)
    # sm.fit(X_all,label_all)
    y_pred = sm.predict(X_test)
    #print (sm.summary())

    tot_perf.append(accuracy_score(y_pred, y_test))
    print (accuracy_score(y_pred, y_test))
print (tot_perf)




