__author__ = 'shyu'

import csv
import scipy
from scipy import sparse
import time

test_mat = sparse.lil_matrix((1000,10000))
test_mat[3,1]=1

test_row = test_mat.getrow(10)
add_row = test_mat.getrow(3)


print (test_row.data)
print (add_row.data)


t0 = time.time()
test_mat[2]=test_row+add_row
t1 = time.time()



total = t1 - t0
print (test_mat)
print (total)


test_mat2 = test_mat.tocsr()
t2 = time.time()
test_mat2[2]=test_mat2.getrow(10)+test_mat2.getrow(3)


t3 = time.time()
total2 = t3-t2
print (total2)
print (test_mat)