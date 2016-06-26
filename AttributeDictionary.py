__author__ = 'shyu'


import operator
import pickle

colid_diag_dict_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_diag_dict.pkl'
colid_proc_dict_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_proc_dict.pkl'
colid_prvd_dict_file =  'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_prvd_dict.pkl'
colid_edipayer_dict_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_edipayer_dict.pkl'
colid_rndring_prvd_dict_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_rndring_prvd_dict.pkl'
colid_remit_srvc_ln_dict_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_remit_srvc_ln_dict.pkl'
colid_rmrk_id_dict_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_rmrk_id_dict.pkl'
colid_adj_rsn_id_dict_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_adj_rsn_id_dict.pkl'
colid_remit_svc_key_dict_file =  'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_remit_svc_key_dict.pkl'
colid_payee_id_dict_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_payee_id_dict.pkl'
colid_patnt_zip_cd_dict_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_patnt_zip_cd_dict.pkl'
colid_patnt_qlfr_cd_dict_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_patnt_qlfr_cd_dict.pkl'
colid_typ_of_bill_dict_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_typ_of_bill_dict.pkl'
colid_cfic_cd_dict_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_cfic_cd_dict.pkl'
colid_subscrbr_gndr_dict_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_subscrbr_gndr_dict.pkl'
colid_subscrbr_qlfr_cd_dict_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_subscrbr_qlfr_cd_dict.pkl'
colid_admssn_typ_cd_dict_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_admssn_typ_cd_dict.pkl'
colid_admssn_src_cd_dict_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_admssn_src_cd_dict.pkl'
colid_patnt_stts_cd_dict_file = 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_patnt_stts_cd_dict.pkl'

def load_object(filepath):
    return pickle.load(open(filepath, 'rb'))



colid_diag_dict = load_object(colid_diag_dict_file)
colid_proc_dict = load_object(colid_proc_dict_file)
colid_prvd_dict = load_object(colid_prvd_dict_file)
colid_edipayer_dict = load_object(colid_edipayer_dict_file)
colid_rndring_prvd_dict = load_object(colid_rndring_prvd_dict_file)
colid_remit_srvc_ln_dict = load_object(colid_remit_srvc_ln_dict_file)
colid_rmrk_id_dict = load_object(colid_rmrk_id_dict_file)
colid_adj_rsn_id_dict = load_object(colid_adj_rsn_id_dict_file)
colid_remit_svc_key_dict = load_object(colid_remit_svc_key_dict_file)
colid_payee_id_dict = load_object(colid_payee_id_dict_file)
colid_patnt_zip_cd_dict = load_object(colid_patnt_zip_cd_dict_file)
colid_patnt_qlfr_cd_dict = load_object(colid_patnt_qlfr_cd_dict_file)
colid_typ_of_bill_dict = load_object(colid_typ_of_bill_dict_file)
colid_cfic_cd_dict = load_object(colid_cfic_cd_dict_file)
colid_subscrbr_gndr_dict = load_object(colid_subscrbr_gndr_dict_file)
colid_subscrbr_qlfr_cd_dict = load_object(colid_subscrbr_qlfr_cd_dict_file)
colid_admssn_typ_cd_dict = load_object(colid_admssn_typ_cd_dict_file)
colid_admssn_src_cd_dict = load_object(colid_admssn_src_cd_dict_file)
colid_patnt_stts_cd_dict = load_object(colid_patnt_stts_cd_dict_file)


sorted_colid_diag_dict = sorted(colid_diag_dict.items(), key=operator.itemgetter(1))
sorted_colid_proc_dict = sorted(colid_proc_dict.items(), key=operator.itemgetter(1))
sorted_colid_prvd_dict = sorted(colid_prvd_dict.items(), key=operator.itemgetter(1))
sorted_colid_edipayer_dict = sorted(colid_edipayer_dict.items(), key=operator.itemgetter(1))
sorted_colid_rndring_prvd_dict = sorted(colid_rndring_prvd_dict.items(), key=operator.itemgetter(1))
sorted_colid_remit_srvc_ln_dict = sorted(colid_remit_srvc_ln_dict.items(), key=operator.itemgetter(1))
sorted_colid_rmrk_id_dict = sorted(colid_rmrk_id_dict.items(), key=operator.itemgetter(1))
sorted_colid_adj_rsn_id_dict = sorted(colid_adj_rsn_id_dict.items(), key=operator.itemgetter(1))
sorted_colid_remit_svc_key_dict = sorted(colid_remit_svc_key_dict.items(), key=operator.itemgetter(1))
sorted_colid_payee_id_dict = sorted(colid_payee_id_dict.items(), key=operator.itemgetter(1))
sorted_colid_patnt_zip_cd_dict = sorted(colid_patnt_zip_cd_dict.items(), key=operator.itemgetter(1))
sorted_colid_patnt_qlfr_cd_dict = sorted(colid_patnt_qlfr_cd_dict.items(), key=operator.itemgetter(1))
sorted_colid_typ_of_bill_dict = sorted(colid_typ_of_bill_dict.items(), key=operator.itemgetter(1))
sorted_colid_cfic_cd_dict = sorted(colid_cfic_cd_dict.items(), key=operator.itemgetter(1))
sorted_subscrbr_gndr_dict = sorted(colid_subscrbr_gndr_dict.items(), key=operator.itemgetter(1))
sorted_subscrbr_qlfr_cd_dict = sorted(colid_subscrbr_qlfr_cd_dict.items(), key=operator.itemgetter(1))
sorted_admssn_typ_cd_dict = sorted(colid_admssn_typ_cd_dict.items(), key=operator.itemgetter(1))
sorted_admssn_src_cd_dict = sorted(colid_admssn_src_cd_dict.items(), key=operator.itemgetter(1))
sorted_patnt_stts_cd_dict = sorted(colid_patnt_stts_cd_dict.items(), key=operator.itemgetter(1))

print (sorted_colid_diag_dict)



labelfile = open('D:\BonSecours\\3weeks\\db_dump\\new_data\\attributes_labels.csv','w')
labelfile.write('position'+'\t'+'Coding Category'+'\t'+'label'+'\n')

attr_count = 1

for k,v in sorted_colid_payee_id_dict:
    labelfile.write(str(attr_count)+'\t'+'payee_id'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1

for k,v in sorted_colid_patnt_zip_cd_dict:
    labelfile.write(str(attr_count)+'\t'+'patnt_zip_cd_id'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1

for k,v in sorted_colid_patnt_qlfr_cd_dict:
    labelfile.write(str(attr_count)+'\t'+'patnt_qlfr_cd'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1

for k,v in sorted_colid_typ_of_bill_dict:
    labelfile.write(str(attr_count)+'\t'+'typ_of_bill'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1

for k,v in sorted_colid_cfic_cd_dict:
    labelfile.write(str(attr_count)+'\t'+'cfic_cd'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1

for k,v in sorted_subscrbr_gndr_dict:
    labelfile.write(str(attr_count)+'\t'+'subscrbr_gndr'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1

for k,v in sorted_subscrbr_qlfr_cd_dict:
    labelfile.write(str(attr_count)+'\t'+'subscrbr_qlfr_cd'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1

for k,v in sorted_admssn_typ_cd_dict:
    labelfile.write(str(attr_count)+'\t'+'admssn_typ_cd'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1

for k,v in sorted_admssn_src_cd_dict:
    labelfile.write(str(attr_count)+'\t'+'admssn_src_cd'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1

for k,v in sorted_patnt_stts_cd_dict:
    labelfile.write(str(attr_count)+'\t'+'patnt_stts_cd'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1

for k,v in sorted_colid_diag_dict:
    labelfile.write(str(attr_count)+'\t'+'diag_cd'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1

for k,v in sorted_colid_proc_dict:
    labelfile.write(str(attr_count)+'\t'+'proc_cd'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1

for k,v in sorted_colid_prvd_dict:
    labelfile.write(str(attr_count)+'\t'+'prvd_cd'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1

labelfile.write(str(attr_count)+'\t'+'Trans 837'+'\t'+'Count Service Times'+'\t'+'0'+'\n')
attr_count = attr_count+1

labelfile.write(str(attr_count)+'\t'+'Trans 837'+'\t'+'Total Line Item Smt Amount'+'\t'+'0'+'\n')
attr_count = attr_count+1

for k,v in sorted_colid_edipayer_dict:
    labelfile.write(str(attr_count)+'\t'+'edipayer_cd'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1

for k,v in sorted_colid_rndring_prvd_dict:
    labelfile.write(str(attr_count)+'\t'+'rndring_prvd'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1

for k,v in sorted_colid_remit_srvc_ln_dict:
    labelfile.write(str(attr_count)+'\t'+'remit_srvc_ln'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1

for k,v in sorted_colid_rmrk_id_dict:
    labelfile.write(str(attr_count)+'\t'+'rmrk_id'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1

for k,v in sorted_colid_adj_rsn_id_dict:
    labelfile.write(str(attr_count)+'\t'+'adj_rsn_id'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1

for k,v in sorted_colid_remit_svc_key_dict:
    labelfile.write(str(attr_count)+'\t'+'remit_svc'+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1



labelfile.close()













