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


tot_unsuc_perf = []
tot_suc_perf = []

print('The scikit-learn version is {}.'.format(sklearn.__version__))

model_data = pickle.load(mat_train)
label_data = pickle.load(mat_label)



indices_unsuc = [i for i, x in enumerate(label_data.getcol(0).data) if ((x == 1.0) | (x ==2.0))]
indices_suc = [i for i, x in enumerate(label_data.getcol(0).data) if ((x == 3.0) | (x ==4.0))]


model_unsuc = model_data[indices_unsuc]
label_unsuc = label_data[indices_unsuc]

model_suc = model_data[indices_suc]
label_suc = label_data[indices_suc]


# for unsuccessful
#
# for loop in range(1,11):
#
#     train_unsuc_ind = np.random.choice(16678,11675, replace=False)
#     test_unsuc_ind = set(range(0,16678))-set(train_unsuc_ind)
#
#     train_unsuc_ind = np.asarray(train_unsuc_ind)
#     test_unsuc_ind = np.asarray(list(test_unsuc_ind))
#
#     model_unsuc_train_data = model_data[train_unsuc_ind,]
#     model_unsuc_test_data = model_data[test_unsuc_ind,]
#
#     label_unsuc_train_data = label_unsuc[train_unsuc_ind,]
#     label_unsuc_test_data = label_unsuc[test_unsuc_ind,]
#
#     #rf = RandomForestClassifier(n_estimators=100, max_depth=5, min_samples_split=100)   #0.631611959022
#     #rf.fit(model_train_data.tocsr(),label_train_data.tocsr().todense())
#     #pred = rf.predict(model_test_data.tocsr())
#
#     #X_, y_ = ds.make_classification(n_samples=200, n_features=100,weights=[0.833, 0.167], random_state=0)
#     #model_data = csr_matrix(model_data)
#     #label_data = label_data[0:100]
#
#
#     X_unsuc_train = scipy.sparse.csr_matrix(model_unsuc_train_data)
#     X_unsuc_test = scipy.sparse.csr_matrix(model_unsuc_test_data)
#
#
#     print (label_unsuc_train_data.get_shape())
#     print (label_unsuc_test_data.get_shape())
#
#     y_unsuc_train = label_unsuc_train_data.todense().reshape((label_unsuc_train_data.shape[0],1))
#     y_unsuc_test = label_unsuc_test_data.todense().reshape((label_unsuc_test_data.shape[0],1))
#
#     #sm = sklearn.linear_model.LogisticRegression()  #0.586702906126
#     sm = sklearn.linear_model.LogisticRegression(penalty='l2',multi_class='multinomial',solver='lbfgs')
#
#     sm.fit(X_unsuc_train,y_unsuc_train)
#     # sm.fit(X_all,label_all)
#     y_unsuc_pred = sm.predict(X_unsuc_test)
#     #print (sm.summary())
#
#     tot_unsuc_perf.append(accuracy_score(y_unsuc_pred, y_unsuc_test))
#     print (accuracy_score(y_unsuc_pred, y_unsuc_test))
# print (tot_unsuc_perf)
#

for loop in range(1,11):

    train_suc_ind = np.random.choice(63042,44136, replace=False)
    test_suc_ind = set(range(0,63042))-set(train_suc_ind)

    train_suc_ind = np.asarray(train_suc_ind)
    test_suc_ind = np.asarray(list(test_suc_ind))

    model_suc_train_data = model_data[train_suc_ind,]
    model_suc_test_data = model_data[test_suc_ind,]

    label_suc_train_data = label_suc[train_suc_ind,]
    label_suc_test_data = label_suc[test_suc_ind,]

    #rf = RandomForestClassifier(n_estimators=100, max_depth=5, min_samples_split=100)   #0.631611959022
    #rf.fit(model_train_data.tocsr(),label_train_data.tocsr().todense())
    #pred = rf.predict(model_test_data.tocsr())

    #X_, y_ = ds.make_classification(n_samples=200, n_features=100,weights=[0.833, 0.167], random_state=0)
    #model_data = csr_matrix(model_data)
    #label_data = label_data[0:100]


    X_suc_train = scipy.sparse.csr_matrix(model_suc_train_data)
    X_suc_test = scipy.sparse.csr_matrix(model_suc_test_data)


    print (label_suc_train_data.get_shape())
    print (label_suc_test_data.get_shape())

    y_suc_train = label_suc_train_data.todense().reshape((label_suc_train_data.shape[0],1))
    y_suc_test = label_suc_test_data.todense().reshape((label_suc_test_data.shape[0],1))

    #sm = sklearn.linear_model.LogisticRegression()  #0.586702906126
    sm = sklearn.linear_model.LogisticRegression(penalty='l2',multi_class='multinomial',solver='lbfgs')

    sm.fit(X_suc_train,y_suc_train)
    # sm.fit(X_all,label_all)
    y_suc_pred = sm.predict(X_suc_test)
    #print (sm.summary())

    tot_suc_perf.append(accuracy_score(y_suc_pred, y_suc_test))
    print (accuracy_score(y_suc_pred, y_suc_test))
print (tot_suc_perf)




