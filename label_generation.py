__author__ = 'shyu'

import csv

account_labels={}
claim_lablels={}

#label definition
# 1:    unsuccessful after many human touches
# 2:    unsuccessful after less human touches
# 3:    successful after many human touches
# 4:    successful after less human touches

with open('D:\\BonSecours\\data_request\\AccountHistory_2\\sample\\ActionBalance.txt') as actionfile:
    reader = csv.reader(actionfile, delimiter='|')
    old_nr = ''
    old_tag = ''
    count_touch = 0
    for line in reader:
        autoNo, account_plain_nr, action_tk, action_detail,  comment, employee, dt, result, tag = line
        #print (autoNo, account_plain_nr, action_tk, action_detail, comment, employee, dt, result, tag)
        if (account_plain_nr != old_nr):
            if ((old_tag is None) | (old_tag =='increase more than double') | (old_tag=='significant increase') | (old_tag=='slightly increase')):
                if (count_touch>3):
                    account_labels[old_nr]='1'
                else:
                    account_labels[old_nr]='2'
            else:
                if (count_touch>3):
                    account_labels[old_nr]='3'
                else:
                    account_labels[old_nr]='4'

            count_touch = 1   # starting with a new touch count
        else:

            count_touch = count_touch+1

        old_nr = account_plain_nr
        old_tag = tag


with open('D:\\BonSecours\\3weeks\\db_dump\\new_data\\mapping_hash_accounts_claimkeys.csv') as keyfile:
    keyreader = csv.reader(keyfile,delimiter=',')
    next(keyreader,None)

    for line in keyreader:
        hashedaccount, plainaccount,claim_keys = line

        if plainaccount not in account_labels:
            print ('Cannot find account')

        else:
            claim_lablels[claim_keys]=account_labels[plainaccount]


labelfile = open('D:\BonSecours\\3weeks\\db_dump\\new_data\\claim_labels.csv','w')
labelfile.write('claim_key,label'+'\n')

for k,v in claim_lablels.items():
    labelfile.write(str(k)+','+str(v)+'\n')




actionfile.close()
keyfile.close()
labelfile.close()