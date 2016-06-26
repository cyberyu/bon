__author__ = 'shyu'

import csv
import scipy
from scipy import sparse
import pickle
import operator


clm_trns_id_dict = {}
colid_diag_dict = {}
colid_proc_dict = {}
colid_prvd_dict = {}


clm_id_dict = {}
colid_edipayer_dict = {}
colid_rndring_prvd_dict ={}

clm_remit_id_dict = {}
colid_remit_srvc_ln_dict = {}
colid_rmrk_id_dict = {}

colid_adj_rsn_id_dict = {}
colid_remit_svc_key_dict = {}

colid_payee_id_dict={}
colid_patnt_zip_cd_dict={}
colid_patnt_qlfr_cd_dict={}
colid_typ_of_bill_dict={}
colid_cfic_cd_dict={}



colid_subscrbr_gndr_dict ={}
colid_subscrbr_qlfr_cd_dict={}
colid_admssn_typ_cd_dict={}
colid_admssn_src_cd_dict={}
colid_patnt_stts_cd_dict={}

mapping_clmkey_to_clmtrnskey_dict = {}
mapping_clmkey_to_clmremitkey_dict = {}



sparse_diag_file = "D:\\BonSecours\\3weeks\\db_dump\\new_data\\claim_diag_query_result.txt"
sparse_proc_file = "D:\\BonSecours\\3weeks\\db_dump\\new_data\\claim_proc_query_result.txt"
sparse_prvd_file = "D:\\BonSecours\\3weeks\\db_dump\\new_data\\claim_prvd_query_result.txt"
sparse_srvc_file = "D:\\BonSecours\\3weeks\\db_dump\\new_data\\claim_srvc_query_result.txt"

sparse_remit_file = "D:\\BonSecours\\3weeks\\db_dump\\new_data\\claim_remit_query_result.txt"
sparse_remit_rmk_file = "D:\\BonSecours\\3weeks\\db_dump\\new_data\\claim_query_remit_rmk_result.txt"
sparse_remit_adj_file = "D:\\BonSecours\\3weeks\\db_dump\\new_data\\claim_remit_adj_query_result.txt"
sparse_remit_srvc_file = "D:\\BonSecours\\3weeks\\db_dump\\new_data\\claim_query_remit_srvc_result.txt"

sparse_clm_file = "D:\\BonSecours\\3weeks\\db_dump\\new_data\\claim_query_result.txt"
sparse_clm_trns_file = "D:\\BonSecours\\3weeks\\db_dump\\new_data\\claim_query_trns_result.txt"

dense_label_file = "D:\\BonSecours\\3weeks\\db_dump\\new_data\\claim_labels.csv"
dense_label_file_binary_unsuc_suc = "D:\\BonSecours\\3weeks\\db_dump\\new_data\\claim_labels_binary_unsuccessful_successful.csv"



#--------------------------LOADING DIAGNOSIS CODE FILE-------------------------------------
print ('--- processing diag code file -----')
csv_diag_reader = csv.reader(open(sparse_diag_file), delimiter=',')
next(csv_diag_reader,None)

for line in csv_diag_reader:
    # CLM_TRNS_FKEY ,DIAG_CD_QLFR ,DIAG_CD_FKEY
    claim_trns_key, diag_cd_qlfr, diag_id = line

    # store the claim key as dictionary keys, and the unique coded id as value
    if claim_trns_key not in clm_trns_id_dict:
        clm_trns_id_dict[claim_trns_key]=len(clm_trns_id_dict)+1

    if diag_id not in colid_diag_dict:
        colid_diag_dict[diag_id]=len(colid_diag_dict)+1

    # store the diag code as dictionary keys, and the unique coded id as value




matrix_diag = sparse.lil_matrix((len(clm_trns_id_dict), len(colid_diag_dict)))

csv_diag_reader = csv.reader(open(sparse_diag_file), delimiter=',')
next(csv_diag_reader,None)
for line in csv_diag_reader:
    claim_trns_key, diag_cd_qlfr, diag_id = line
    matrix_diag[clm_trns_id_dict[claim_trns_key]-1,colid_diag_dict[diag_id]-1]=1



#---------------LOADING PROCEDURE CODE FILE----------------------------------------
print ('--- processing procedure code file -----')
csv_proc_reader = csv.reader(open(sparse_proc_file), delimiter=',')
next(csv_proc_reader,None)

for line in csv_proc_reader:
    #CLM_TRNS_FKEY ,CLM_TRNS_SRVC_LN_FKEY ,PROC_PRRTY ,PROC_CD
    claim_trns_key, claim_srvs_key ,proc_prrty, proc_id = line
    if proc_id not in colid_proc_dict:
        colid_proc_dict[proc_id] = len(colid_proc_dict)+1
    if claim_trns_key not in clm_trns_id_dict:
        print ('Error: a claim key is not found in the claim key dictionary constructed from diag file')
        quit()


matrix_proc = sparse.lil_matrix((len(clm_trns_id_dict), len(colid_proc_dict)))

csv_proc_reader = csv.reader(open(sparse_proc_file), delimiter=',')
next(csv_diag_reader,None)
for line in csv_diag_reader:
    claim_trns_key, claim_srvs_key ,proc_prrty, proc_id = line
    matrix_proc[clm_trns_id_dict[claim_trns_key]-1,colid_proc_dict[proc_id]-1]=1


#---------------LOADING PROVIDER CODE FILE----------------------------------------
print ('--- processing provider code file -----')
csv_prvd_reader = csv.reader(open(sparse_prvd_file), delimiter=',')
next(csv_prvd_reader,None)

# construct the dictionary
for line in csv_prvd_reader:
    #CLM_TRNS_FKEY ,PRVDR_NPI ,PRVDR_TYP_CD_FKEY ,PRVDR_FIRST_NM ,PRVDR_LAST_NM
    claim_trns_key, prvdr_npi, prvdr_cd, prvdr_fn, prvdr_ln = line
    if prvdr_npi not in colid_prvd_dict:
        colid_prvd_dict[prvdr_npi]=len(prvdr_npi)+1
    if claim_trns_key not in clm_trns_id_dict:
        print ('Error: a claim trans key is not found in the claim trans key dictionary constructed from diag file')
        quit()


matrix_prvd = sparse.lil_matrix((len(clm_trns_id_dict), len(colid_prvd_dict)))

# assign the sparse matrix
csv_prvd_reader = csv.reader(open(sparse_prvd_file), delimiter=',')
next(csv_prvd_reader,None)
for line in csv_prvd_reader:
    #CLM_TRNS_FKEY ,PRVDR_NPI ,PRVDR_TYP_CD_FKEY ,PRVDR_FIRST_NM ,PRVDR_LAST_NM
    claim_trns_key, prvdr_npi, prvdr_cd, prvdr_fn, prvdr_ln = line
    matrix_prvd[clm_trns_id_dict[claim_trns_key]-1,colid_prvd_dict[prvdr_npi]-1]=1




#---------------LOADING Service CODE FILE----------------------------------------
print ('--- processing serviice code file -----')

matrix_srvc = sparse.lil_matrix((len(clm_trns_id_dict), 2))

csv_srvc_reader = csv.reader(open(sparse_srvc_file), delimiter=',')
next(csv_srvc_reader,None)


# for each clm_trns_key, we count how many srvc_ln_nbr are assoicated to that, and calculate the average and sum of
# submission amount
for line in csv_srvc_reader:
    claim_trns_key, srvc_ln_nbr, svc_strt_dt, srvc_end_dt, ln_item_sbmt_amt = line
    if claim_trns_key in clm_trns_id_dict:

        # count the srv times
        matrix_srvc[clm_trns_id_dict[claim_trns_key]-1,0]=matrix_srvc[clm_trns_id_dict[claim_trns_key]-1,0]+1
        # sum up the amount
        matrix_srvc[clm_trns_id_dict[claim_trns_key]-1,1]=matrix_srvc[clm_trns_id_dict[claim_trns_key]-1,1]+float(ln_item_sbmt_amt)

    else:
        matrix_srvc[clm_trns_id_dict[claim_trns_key]-1,0]=1
        matrix_srvc[clm_trns_id_dict[claim_trns_key]-1,1]=float(ln_item_sbmt_amt)


#----------------------------LOADING REMIT QUERY FILE----------------------------------------
print ('--- processing claim remit file -----')
csv_remit_reader = csv.reader(open(sparse_remit_file), delimiter='\t')
next(csv_remit_reader,None)

# construct the dictionary for EDI_PAYEE and RNDRING_PRVDR_ID
for line in csv_remit_reader:
    #CLM_FKEY ,CLM_REMIT_DT ,EDI_PAYER_FKEY ,RNDRING_PRVDR_NM ,RNDRING_PRVDR_ID ,CLM_PAYMT_AMT ,CLM_TOT_CHRG_AMT ,PATNT_RESPNBLTY_AMT

    claim_remit_key, claim_key, clm_remit_dt, edi_payer_id, rndring_prvd_nm, rndring_prvd_id, clm_paymt_amt, clm_tot_chrg_amt, pantnt_resp_amt = line
    if claim_remit_key not in clm_remit_id_dict:
        clm_remit_id_dict[claim_remit_key]=len(clm_remit_id_dict)+1
    if claim_key not in clm_id_dict:
        clm_id_dict[claim_key]=len(clm_id_dict)+1
    if claim_remit_key in mapping_clmkey_to_clmremitkey_dict:
        print ('Error: claim remit key is not unique ')
        quit()
    else:
        mapping_clmkey_to_clmremitkey_dict[claim_remit_key]=claim_key

    if edi_payer_id not in colid_edipayer_dict:
        colid_edipayer_dict[edi_payer_id]=len(colid_edipayer_dict)+1
    if rndring_prvd_id not in colid_rndring_prvd_dict:
        colid_rndring_prvd_dict[rndring_prvd_id] = len(colid_rndring_prvd_dict)+1


matrix_edipayer = sparse.lil_matrix((len(clm_remit_id_dict), len(colid_edipayer_dict)))
matrix_rndring_prvd = sparse.lil_matrix((len(clm_remit_id_dict), len(colid_rndring_prvd_dict)))

for line in csv_remit_reader:
    #CLM_FKEY ,CLM_REMIT_DT ,EDI_PAYER_FKEY ,RNDRING_PRVDR_NM ,RNDRING_PRVDR_ID ,CLM_PAYMT_AMT ,CLM_TOT_CHRG_AMT ,PATNT_RESPNBLTY_AMT
    claim_key, clm_remit_dt, edi_payer_id, rndring_prvd_nm, rndring_prvd_id, clm_paymt_amt, clm_tot_chrg_amt, pantnt_resp_amt = line
    matrix_edipayer[clm_id_dict[claim_trns_key]-1,colid_edipayer_dict[edi_payer_id]-1]=1.0
    matrix_rndring_prvd[clm_id_dict[claim_trns_key]-1,colid_rndring_prvd_dict[rndring_prvd_id]]=1.0




#----------------------------LOADING REMIT REMARK QUERY FILE----------------------------------------
print ('--- processing claim remit remark, service and adjustment files -----')
csv_remit_rmk_reader = csv.reader(open(sparse_remit_rmk_file), delimiter=',')
next(csv_remit_rmk_reader,None)

for line in csv_remit_rmk_reader:
    #CLM_REMIT_FKEY ,CLM_REMIT_SRVC_LN_FKEY ,RMRK_CD_FKEY ,RMRK_SGMT

    clm_remit_key, clm_remit_srv_ln_key, rmrk_id, rmrk_sgmt = line
    if clm_remit_key not in clm_remit_id_dict:
        clm_remit_id_dict[clm_remit_key] = len(clm_remit_id_dict)+1

    # if clm_remit_srv_ln_key not in colid_remit_srvc_ln_dict:
    #     colid_remit_srvc_ln_dict[clm_remit_srv_ln_key] = len(colid_remit_srvc_ln_dict)+1

    if rmrk_id not in colid_rmrk_id_dict:
        colid_rmrk_id_dict[rmrk_id] = len(colid_rmrk_id_dict)+1


csv_remit_adj_reader = csv.reader(open(sparse_remit_adj_file), delimiter=',')
next(csv_remit_adj_reader,None)

for line in csv_remit_adj_reader:
    #CLM_REMIT_FKEY ,CLM_REMIT_SRVC_LN_FKEY ,CLM_ADJ_RSN_CD_FKEY ,CLM_ADJ_GRP_CD_FKEY ,ADJ_AMT ,ADJ_QTY
    clm_remit_key, clm_remit_srv_ln_key, clm_adj_rsn_id, clm_adj_grp_id, adj_amt, adj_qt = line

    if clm_remit_key not in clm_remit_id_dict:
        clm_remit_id_dict[clm_remit_key] = len(clm_remit_id_dict)+1

    if clm_adj_rsn_id not in colid_adj_rsn_id_dict:
        colid_adj_rsn_id_dict[clm_adj_rsn_id]=len(colid_adj_rsn_id_dict)+1


csv_remit_srvc_reader = csv.reader(open(sparse_remit_srvc_file), delimiter=',')
next(csv_remit_srvc_reader,None)

for line in csv_remit_srvc_reader:
    #CLM_REMIT_FKEY ,SRVC_CD ,SRVC_STRT_DT ,SRVC_END_DT ,SRVC_CD_SRC ,REV_CD_FKEY ,LN_ITEM_SBMT_AMT ,LN_ITEM_PYMNT_AMNT ,SRVC_UNIT_CNT_ORIG ,SRVC_UNIT_CNT_PAID
    clm_remit_key, srvc_cd, srvc_strt_dt, srvc_end_dt, srvc_cd_src, rev_cd_key, ln_item_sbmt_amt, ln_itm_pymnt_amt, srvc_unit_cnt_orig, srvc_unit_cnt_paid = line
    if clm_remit_key not in clm_remit_id_dict:
        clm_remit_id_dict[clm_remit_key] = len(clm_remit_id_dict)+1
    if srvc_cd not in colid_remit_svc_key_dict:
        colid_remit_svc_key_dict[srvc_cd] = len(colid_remit_svc_key_dict)+1

#matrix_remit_srvc = sparse.lil_matrix((len(clm_remit_id_dict), len(colid_remit_srvc_ln_dict)))
matrix_remit_rmrk = sparse.lil_matrix((len(clm_remit_id_dict), len(colid_rmrk_id_dict)))
matrix_remit_adj = sparse.lil_matrix((len(clm_remit_id_dict), len(colid_adj_rsn_id_dict)))
matrix_remit_srvc = sparse.lil_matrix((len(clm_remit_id_dict), len(colid_remit_svc_key_dict)))


csv_remit_rmk_reader = csv.reader(open(sparse_remit_rmk_file), delimiter=',')
next(csv_remit_rmk_reader,None)

for line in csv_remit_rmk_reader:
    clm_remit_key, clm_remit_srv_ln_key, rmrk_id, rmrk_sgmt = line
    #print (line)
    #matrix_remit_srvc[clm_remit_id_dict[clm_remit_key]-1,colid_remit_srvc_ln_dict[clm_remit_srv_ln_key]-1]=1.0
    matrix_remit_rmrk[clm_remit_id_dict[clm_remit_key]-1,colid_rmrk_id_dict[rmrk_id]-1]=1.0


#----------------------------LOADING REMIT ADJ QUERY FILE----------------------------------------
csv_remit_adj_reader = csv.reader(open(sparse_remit_adj_file), delimiter=',')
next(csv_remit_adj_reader,None)

for line in csv_remit_adj_reader:
    #CLM_REMIT_FKEY ,CLM_REMIT_SRVC_LN_FKEY ,CLM_ADJ_RSN_CD_FKEY ,CLM_ADJ_GRP_CD_FKEY ,ADJ_AMT ,ADJ_QTY
    clm_remit_key, clm_remit_srv_ln_key, clm_adj_rsn_id, clm_adj_grp_id, adj_amt, adj_qt = line
    # store the adjust amount in the non-sparse field
    matrix_remit_adj[clm_remit_id_dict[clm_remit_key]-1,colid_adj_rsn_id_dict[clm_adj_rsn_id]-1]=float(adj_amt)



csv_remit_srvc_reader = csv.reader(open(sparse_remit_srvc_file), delimiter=',')
next(csv_remit_srvc_reader,None)

for line in csv_remit_srvc_reader:
    #CLM_REMIT_FKEY ,SRVC_CD ,SRVC_STRT_DT ,SRVC_END_DT ,SRVC_CD_SRC ,REV_CD_FKEY ,LN_ITEM_SBMT_AMT ,LN_ITEM_PYMNT_AMNT ,SRVC_UNIT_CNT_ORIG ,SRVC_UNIT_CNT_PAID
    clm_remit_key, srvc_cd, srvc_strt_dt, srvc_end_dt, srvc_cd_src, rev_cd_key, ln_item_sbmt_amt, ln_itm_pymnt_amt, srvc_unit_cnt_orig, srvc_unit_cnt_paid = line
    matrix_remit_srvc[clm_remit_id_dict[clm_remit_key]-1,colid_remit_svc_key_dict[srvc_cd]-1] = float(ln_item_sbmt_amt)


csv_clm_reader = csv.reader(open(sparse_clm_file), delimiter='\t')
next(csv_clm_reader, None)

print ('--- processing claim file -----')

for line in csv_clm_reader:
    #CLM_PKEY ,  PATNT_CNTRL_NBR ,  PAYEE_ID ,  PAYEE_NM ,  PATNT_MED_RCRD_NBR ,PATNT_BRTH_DT ,PATNT_ZIP_CD_FKEY ,PATNT_QLFR_CD ,TYP_OF_BILL ,CFIC_CD ,CLM_ADMIT_DT ,CLM_DSCHRG_DT
    clm_key, patnt_cntrl_nbr, payee_id, payee_nm, patnt_med_rcrd_nbr, patnt_brth_dt, patnt_zip_cd, patnt_qlfr_cd, typ_of_bill, cfic_cd, clm_admit_dt, clm_dischrg_dt = line
    if claim_key not in clm_id_dict:
        clm_id_dict[claim_key]=len(clm_id_dict)+1
    if payee_id not in colid_payee_id_dict:
        colid_payee_id_dict[payee_id]=len(colid_payee_id_dict)+1
    if patnt_zip_cd not in colid_patnt_zip_cd_dict:
        colid_patnt_zip_cd_dict[patnt_zip_cd] = len(colid_patnt_zip_cd_dict)+1
    if patnt_qlfr_cd not in colid_patnt_qlfr_cd_dict:
        colid_patnt_qlfr_cd_dict[patnt_qlfr_cd] = len(colid_patnt_qlfr_cd_dict)+1
    if typ_of_bill not in colid_typ_of_bill_dict:
        colid_typ_of_bill_dict[typ_of_bill] = len(colid_typ_of_bill_dict)+1
    if cfic_cd not in colid_cfic_cd_dict:
        colid_cfic_cd_dict[cfic_cd] = len(colid_cfic_cd_dict)+1


matrix_clm_payee = sparse.lil_matrix((len(clm_id_dict), len(colid_payee_id_dict)))
matrix_clm_patnt_zip = sparse.lil_matrix((len(clm_id_dict), len(colid_patnt_zip_cd_dict)))
matrix_clm_patnt_qlfr_cd = sparse.lil_matrix((len(clm_id_dict), len(colid_patnt_qlfr_cd_dict)))
matrix_clm_typ_of_bill = sparse.lil_matrix((len(clm_id_dict), len(colid_typ_of_bill_dict)))
matrix_clm_cfic_cd = sparse.lil_matrix((len(clm_id_dict), len(colid_cfic_cd_dict)))
#matrix_clm_dense = sparse.lil_matrix((len(clm_id_dict), 3))

csv_clm_reader = csv.reader(open(sparse_clm_file), delimiter='\t')
next(csv_clm_reader, None)


for line in csv_clm_reader:
    #CLM_PKEY ,  PATNT_CNTRL_NBR ,  PAYEE_ID ,  PAYEE_NM ,  PATNT_MED_RCRD_NBR ,PATNT_BRTH_DT ,PATNT_ZIP_CD_FKEY ,PATNT_QLFR_CD ,TYP_OF_BILL ,CFIC_CD ,CLM_ADMIT_DT ,CLM_DSCHRG_DT
    clm_key, patnt_cntrl_nbr, payee_id, payee_nm, patnt_med_rcrd_nbr, patnt_brth_dt, patnt_zip_cd, patnt_qlfr_cd, typ_of_bill, cfic_cd, clm_admit_dt, clm_dischrg_dt = line
    matrix_clm_payee[clm_id_dict[clm_key]-1,colid_payee_id_dict[payee_id]-1] = 1
    matrix_clm_patnt_zip[clm_id_dict[clm_key]-1,colid_patnt_zip_cd_dict[patnt_zip_cd]-1] = 1
    matrix_clm_patnt_qlfr_cd[clm_id_dict[clm_key]-1,colid_patnt_qlfr_cd_dict[patnt_qlfr_cd]-1] = 1
    matrix_clm_typ_of_bill[clm_id_dict[clm_key]-1,colid_typ_of_bill_dict[typ_of_bill]-1] = 1
    matrix_clm_cfic_cd[clm_id_dict[clm_key]-1,colid_cfic_cd_dict[cfic_cd]-1] = 1
    #matrix_clm_dense[clm_id_dict[clm_key]-1,0] = patnt_brth_dt
    #matrix_clm_dense[clm_id_dict[clm_key]-1,1] = clm_admit_dt
    #matrix_clm_dense[clm_id_dict[clm_key]-1,2] = clm_dischrg_dt

print ('--- processing claim trns file -----')

csv_clm_trns_reader = csv.reader(open(sparse_clm_trns_file), delimiter=',')
next(csv_clm_trns_reader, None)

for line in csv_clm_trns_reader:
    #CLM_FKEY ,CLM_TRNS_PKEY ,TRNSCTN_SET_CRTN_DT ,PAYER_RSPSBLTY_SEQ_NBR_CD ,SUBSCRBR_BRTH_DT ,SUBSCRBR_GNDR ,SUBSCRBR_QLFR_CD ,SRVC_AUTHRZTN_EXCPTN_CD ,RFRRL_NBR ,PRIOR_AUTHRZTN_NBR ,CLM_TOT_CHRG_AMT ,CLM_TRNS_ADMIT_DT ,ADMSSN_TYP_CD ,ADMSSN_SRC_CD ,PATNT_STTS_CD ,CLM_TRNS_DRG_CD_FKEY
    claim_key,claim_trns_key,trnsctn_set_crtn_dt, payer_rspsblty_seo_nbr_cd, subscrbr_brth_dt, subscrbr_gndr,subscrbr_qlfr_cd, srvc_authrztn_excpth_cd, rfprl_nbr, prior_authrztn_nbr, clm_tot_chrg_amt, clm_trns_admit_dt, admssn_typ_cd,  admssn_src_cd, patnt_stts_cd, clm_trns_drg_cd_fkey = line

    if claim_trns_key in mapping_clmkey_to_clmtrnskey_dict:
        print ('Error: claim trans mapping is not unique')
        quit()
    else:
        mapping_clmkey_to_clmtrnskey_dict[claim_trns_key]=claim_key

    if claim_key not in clm_id_dict:
        clm_id_dict[claim_key]=len(clm_id_dict)+1

    if subscrbr_gndr not in colid_subscrbr_gndr_dict:
        colid_subscrbr_gndr_dict[subscrbr_gndr]=len(colid_subscrbr_gndr_dict)+1

    if subscrbr_qlfr_cd not in colid_subscrbr_qlfr_cd_dict:
        colid_subscrbr_qlfr_cd_dict[subscrbr_qlfr_cd] = len(colid_subscrbr_qlfr_cd_dict)+1

    if admssn_typ_cd not in colid_admssn_typ_cd_dict:
        colid_admssn_typ_cd_dict[admssn_typ_cd] = len(colid_admssn_typ_cd_dict) + 1

    if admssn_src_cd not in colid_admssn_src_cd_dict:
        colid_admssn_src_cd_dict[admssn_src_cd] = len(colid_admssn_src_cd_dict) + 1

    if patnt_stts_cd not in colid_patnt_stts_cd_dict:
        colid_patnt_stts_cd_dict[patnt_stts_cd] = len(colid_patnt_stts_cd_dict) + 1


matrix_clm_trns_gndr = sparse.lil_matrix((len(clm_id_dict), len(colid_subscrbr_gndr_dict)))
matrix_clm_trns_qlfr_cd = sparse.lil_matrix((len(clm_id_dict), len(colid_subscrbr_qlfr_cd_dict)))
matrix_admssn_typ_cd = sparse.lil_matrix((len(clm_id_dict), len(colid_admssn_typ_cd_dict)))
matrix_admssn_src_cd = sparse.lil_matrix((len(clm_id_dict), len(colid_admssn_src_cd_dict)))
matrix_patnt_stts_cd = sparse.lil_matrix((len(clm_id_dict), len(colid_patnt_stts_cd_dict)))


csv_clm_trns_reader = csv.reader(open(sparse_clm_trns_file), delimiter=',')
next(csv_clm_trns_reader, None)

for line in csv_clm_trns_reader:
    claim_key,claim_trns_key,trnsctn_set_crtn_dt, payer_rspsblty_seo_nbr_cd, subscrbr_brth_dt, subscrbr_gndr,subscrbr_qlfr_cd, srvc_authrztn_excpth_cd, rfprl_nbr, prior_authrztn_nbr, clm_tot_chrg_amt, clm_trns_admit_dt, admssn_typ_cd,  admssn_src_cd, patnt_stts_cd, clm_trns_drg_cd_fkey = line

    matrix_clm_trns_gndr[clm_id_dict[clm_key]-1,colid_subscrbr_gndr_dict[subscrbr_gndr]-1] = 1
    matrix_clm_trns_qlfr_cd[clm_id_dict[clm_key]-1,colid_subscrbr_qlfr_cd_dict[subscrbr_qlfr_cd]-1] = 1
    matrix_admssn_typ_cd[clm_id_dict[clm_key]-1,colid_admssn_typ_cd_dict[admssn_typ_cd]-1] = 1
    matrix_admssn_src_cd[clm_id_dict[clm_key]-1,colid_admssn_src_cd_dict[admssn_src_cd]-1] = 1
    matrix_patnt_stts_cd[clm_id_dict[clm_key]-1,colid_patnt_stts_cd_dict[patnt_stts_cd]-1] = 1

print (matrix_diag.get_dense_label_file)           # 119159x6609
print (matrix_proc.get_shape)           # 119159x4974
print (matrix_prvd.get_shape)           # 119159x4919
print (matrix_srvc.get_shape)           # 119159x2
print (matrix_edipayer.get_shape)       # 166335x3947
print (matrix_rndring_prvd.get_shape)   # 166335x1001
print (matrix_remit_rmrk.get_shape)     # 166335x337
print (matrix_remit_adj.get_shape)      # 166335x158
print (matrix_remit_srvc.get_shape)     # 166335x4194
print (matrix_clm_payee.get_shape)      # 79721x60
print (matrix_clm_patnt_zip.get_shape)  # 79721x761
print (matrix_clm_patnt_qlfr_cd.get_shape)  # 79721x6
print (matrix_clm_typ_of_bill.get_shape)  # 79721x59
print (matrix_clm_cfic_cd.get_shape)    # 79721x8
print (matrix_clm_trns_gndr.get_shape)  # 79721x3
print (matrix_clm_trns_qlfr_cd.get_shape)  # 79721x2
print (matrix_admssn_typ_cd.get_shape)  # 79721x7
print (matrix_admssn_src_cd.get_shape)  # 79721x11
print (matrix_patnt_stts_cd.get_shape)  # 79721x28

print ('-----Key mapping---')
print (len(mapping_clmkey_to_clmtrnskey_dict))
print (len(mapping_clmkey_to_clmremitkey_dict))

print (matrix_patnt_stts_cd.shape)
print (matrix_patnt_stts_cd.shape[0])
print (matrix_patnt_stts_cd.shape[1])

print ('--- aggregating sparse matrices -----')

# stack all the sparse attributes
# stacking at claim trns key level
# stacking five sources:  diag code, proc code, prvd code, srvc line count, srvc amt sum

tot_clm_trns_mat = sparse.hstack((matrix_diag,matrix_proc), format='lil', dtype='float')
del matrix_diag
del matrix_proc
print (tot_clm_trns_mat.shape)
tot_clm_trns_mat = sparse.hstack((tot_clm_trns_mat,matrix_prvd), format='lil', dtype='float')
print (tot_clm_trns_mat.shape)
del matrix_prvd
tot_clm_trns_mat = sparse.hstack((tot_clm_trns_mat,matrix_srvc), format='lil', dtype='float')
del matrix_srvc
print (tot_clm_trns_mat.shape)
#tot_clm_trns_attr = matrix_diag.get_shape

tot_clm_remit_mat = sparse.hstack((matrix_edipayer,matrix_rndring_prvd),format='lil', dtype='float')
del matrix_edipayer
del matrix_rndring_prvd
tot_clm_remit_mat = sparse.hstack((tot_clm_remit_mat,matrix_remit_rmrk),format='lil', dtype='float')
del matrix_remit_rmrk
tot_clm_remit_mat = sparse.hstack((tot_clm_remit_mat,matrix_remit_adj),format='lil', dtype='float')
del matrix_remit_adj
tot_clm_remit_mat = sparse.hstack((tot_clm_remit_mat,matrix_remit_srvc),format='lil', dtype='float')
del matrix_remit_srvc
print (tot_clm_remit_mat.shape)
# aggregate claim_trans_key  to claim_key

tot_clm_mat = sparse.hstack((matrix_clm_payee,matrix_clm_patnt_zip), format='lil', dtype='float')
del matrix_clm_payee
del matrix_clm_patnt_zip
tot_clm_mat = sparse.hstack((tot_clm_mat,matrix_clm_patnt_qlfr_cd), format='lil', dtype='float')
del matrix_clm_patnt_qlfr_cd
tot_clm_mat = sparse.hstack((tot_clm_mat,matrix_clm_typ_of_bill), format='lil', dtype='float')
del matrix_clm_typ_of_bill
tot_clm_mat = sparse.hstack((tot_clm_mat,matrix_clm_cfic_cd), format='lil', dtype='float')
del matrix_clm_cfic_cd
tot_clm_mat = sparse.hstack((tot_clm_mat,matrix_clm_trns_gndr), format='lil', dtype='float')
del matrix_clm_trns_gndr
tot_clm_mat = sparse.hstack((tot_clm_mat,matrix_clm_trns_qlfr_cd), format='lil', dtype='float')
del matrix_clm_trns_qlfr_cd
tot_clm_mat = sparse.hstack((tot_clm_mat,matrix_admssn_typ_cd), format='lil', dtype='float')
del matrix_admssn_typ_cd
tot_clm_mat = sparse.hstack((tot_clm_mat,matrix_admssn_src_cd), format='lil', dtype='float')
del matrix_admssn_src_cd
tot_clm_mat = sparse.hstack((tot_clm_mat,matrix_patnt_stts_cd), format='lil', dtype='float')
del matrix_patnt_stts_cd
print (tot_clm_mat.shape)



print ('---relate claim trns level information to claim level')
tot_clm_trns_to_clm = sparse.lil_matrix((tot_clm_mat.shape[0],tot_clm_trns_mat.shape[1]))

count = 0
for k,v in mapping_clmkey_to_clmtrnskey_dict.items():
    # k= clm_trns_key,  v = clm_key
    # find the original locationof k in tot_clm_trns_mat
    #temp1 = tot_clm_trns_to_clm[clm_id_dict[v]-1].tocsr()
    #temp2 = temp1.tocoo() + tot_clm_trns_mat_coo.getrow(clm_trns_id_dict[k]-1)
    print ('iterating '+ str(count)+ '-th element in mapping claim to claim trans dict' )
    count = count +1
    tot_clm_trns_to_clm[clm_id_dict[v]-1] = tot_clm_trns_to_clm[clm_id_dict[v]-1] + tot_clm_trns_mat[clm_trns_id_dict[k]-1]


tot_clm_remit_to_clm = sparse.lil_matrix((tot_clm_mat.shape[0],tot_clm_remit_mat.shape[1]))

count = 0
for k,v in mapping_clmkey_to_clmremitkey_dict.items():
    # k = clm_remit_key,  v = clm_key
    # find the original locationof k in tot_clm_remit_mat
    print ('iterating '+ str(count)+ '-th element in mapping claim to claim remit dict' )
    count = count +1
    tot_clm_remit_to_clm[clm_id_dict[v]-1]=tot_clm_remit_to_clm[clm_id_dict[v]-1]+tot_clm_remit_mat[clm_remit_id_dict[k]-1]


tot_clm_label = sparse.lil_matrix((tot_clm_mat.shape[0],1))

csv_clm_label_reader = csv.reader(open(dense_label_file), delimiter=',')
next(csv_clm_label_reader, None)

for line in csv_clm_label_reader:
    n_claim_key, claim_label = line

    if n_claim_key not in clm_id_dict:
        print ('Error: non found claim key')

    else:
        tot_clm_label[clm_id_dict[n_claim_key]-1,0] = claim_label



print ('Stack again')
# stack all the matrices again
total_mat = sparse.hstack((tot_clm_mat,tot_clm_trns_to_clm),format='lil',dtype ='float')
del tot_clm_mat
del tot_clm_trns_to_clm
total_mat = sparse.hstack((total_mat,tot_clm_remit_to_clm),format='lil',dtype ='float')
del tot_clm_remit_to_clm
# attaching label as the final column
total_train = total_mat
total_mat = sparse.hstack((total_mat,tot_clm_label),format='lil',dtype ='float')

print (total_mat.shape)

print ('Saving Pickle outputs')
def save_object(obj,filename):
    with open(filename, 'wb') as output:
        pickle.dump(obj, output, pickle.HIGHEST_PROTOCOL)


# generating attribute maps
# sort all the indicator dictionaries by their values
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

save_object(total_mat, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\total_mat.pkl')
save_object(total_train, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\total_train.pkl')
save_object(tot_clm_label, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\total_label.pkl')
save_object(clm_id_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\clm_id_dict.pkl')
save_object(colid_diag_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_diag_dict.pkl')
save_object(colid_proc_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_proc_dict.pkl')
save_object(colid_prvd_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_prvd_dict.pkl')
save_object(colid_edipayer_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_edipayer_dict.pkl')
save_object(colid_rndring_prvd_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_rndring_prvd_dict.pkl')
save_object(colid_remit_srvc_ln_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_remit_srvc_ln_dict.pkl')
save_object(colid_rmrk_id_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_rmrk_id_dict.pkl')
save_object(colid_adj_rsn_id_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_adj_rsn_id_dict.pkl')
save_object(colid_remit_svc_key_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_remit_svc_key_dict.pkl')
save_object(colid_payee_id_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_payee_id_dict.pkl')
save_object(colid_patnt_zip_cd_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_patnt_zip_cd_dict.pkl')
save_object(colid_patnt_qlfr_cd_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_patnt_qlfr_cd_dict.pkl')
save_object(colid_typ_of_bill_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_typ_of_bill_dict.pkl')
save_object(colid_cfic_cd_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_cfic_cd_dict.pkl')
save_object(colid_subscrbr_gndr_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_subscrbr_gndr_dict.pkl')
save_object(colid_subscrbr_qlfr_cd_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_subscrbr_qlfr_cd_dict.pkl')
save_object(colid_admssn_typ_cd_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_admssn_typ_cd_dict.pkl')
save_object(colid_admssn_src_cd_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_admssn_src_cd_dict.pkl')
save_object(colid_patnt_stts_cd_dict, 'D:\\BonSecours\\3weeks\\db_dump\\new_data\\colid_patnt_stts_cd_dict.pkl')



labelfile = open('D:\BonSecours\\3weeks\\db_dump\\new_data\\attributes_labels.csv','w')
labelfile.write('position'+'\t'+'label'+'\n')

attr_count = 1

for k,v in sorted_colid_payee_id_dict:
    labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
    attr_count = attr_count+1
#
# for k,v in sorted_colid_patnt_zip_cd_dict.items():
#     labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
#     attr_count = attr_count+1
#
# for k,v in sorted_colid_patnt_qlfr_cd_dict.items():
#     labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
#     attr_count = attr_count+1
#
# for k,v in sorted_colid_typ_of_bill_dict.items():
#     labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
#     attr_count = attr_count+1
#
# for k,v in sorted_colid_cfic_cd_dict.items():
#     labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
#     attr_count = attr_count+1
#
# for k,v in sorted_subscrbr_gndr_dict.items():
#     labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
#     attr_count = attr_count+1
#
# for k,v in sorted_subscrbr_qlfr_cd_dict.items():
#     labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
#     attr_count = attr_count+1
#
# for k,v in sorted_admssn_typ_cd_dict.items():
#     labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
#     attr_count = attr_count+1
#
# for k,v in sorted_admssn_src_cd_dict.items():
#     labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
#     attr_count = attr_count+1
#
# for k,v in sorted_patnt_stts_cd_dict.items():
#     labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
#     attr_count = attr_count+1
#
# for k,v in sorted_colid_diag_dict.items():
#     labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
#     attr_count = attr_count+1
#
# for k,v in sorted_colid_proc_dict.items():
#     labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
#     attr_count = attr_count+1
#
# for k,v in sorted_colid_prvd_dict.items():
#     labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
#     attr_count = attr_count+1
#
# labelfile.write(str(attr_count)+'\t'+'Count Serice Times'+'\t'+'0'+'\n')
# attr_count = attr_count+1
#
# labelfile.write(str(attr_count)+'\t'+'Total Line Item Smt Amount'+'\t'+'0'+'\n')
# attr_count = attr_count+1
#
# for k,v in sorted_colid_edipayer_dict.items():
#     labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
#     attr_count = attr_count+1
#
# for k,v in sorted_colid_rndring_prvd_dict.items():
#     labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
#     attr_count = attr_count+1
#
# for k,v in sorted_colid_remit_srvc_ln_dict.items():
#     labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
#     attr_count = attr_count+1
#
# for k,v in sorted_colid_rmrk_id_dict.items():
#     labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
#     attr_count = attr_count+1
#
# for k,v in sorted_colid_adj_rsn_id_dict.items():
#     labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
#     attr_count = attr_count+1
#
# for k,v in sorted_colid_remit_svc_key_dict.items():
#     labelfile.write(str(attr_count)+'\t'+str(k)+'\t'+str(v)+'\n')
#     attr_count = attr_count+1
#
#

labelfile.close()
















