__author__ = 'shyu'


import pypyodbc as pyodbc
import csv

batchsize = 10000
con_string = 'DSN=%s;UID=%s;PWD=%s;DATABASE=%s;' % ('BonSecours_Intellect','RI_BonSecours_Reader','ibm83gbs!','RI_BonSecours')
diag_q = 'select [CLM_TRNS_FKEY],[DIAG_CD_QLFR],[DIAG_CD_FKEY] from [DW].[CLM_TRNS_DIAG] WHERE [CLM_TRNS_FKEY] IN (%s)'
proc_q = 'select [CLM_TRNS_FKEY],[CLM_TRNS_SRVC_LN_FKEY],[PROC_PRRTY],[PROC_CD] from [DW].[CLM_TRNS_PROC] WHERE [CLM_TRNS_FKEY] IN (%s)'
prvd_q = 'select [CLM_TRNS_FKEY],[PRVDR_NPI],[PRVDR_TYP_CD_FKEY],[PRVDR_FIRST_NM],[PRVDR_LAST_NM] from [DW].[CLM_TRNS_PRVDR] WHERE [CLM_TRNS_FKEY] IN (%s)'
srvc_q = 'select [CLM_TRNS_FKEY],[SRVC_LN_NBR],[SRVC_STRT_DT],[SRVC_END_DT],[LN_ITEM_SBMT_AMT] from [DW].[CLM_TRNS_SRVC_LN] where [CLM_TRNS_FKEY] IN (%s)'
remit_q = 'select [CLM_REMIT_PKEY],[CLM_FKEY],[CLM_REMIT_DT],[EDI_PAYER_FKEY],[RNDRING_PRVDR_NM],[RNDRING_PRVDR_ID],[CLM_PAYMT_AMT],[CLM_TOT_CHRG_AMT],[PATNT_RESPNBLTY_AMT] from [RI_BonSecours].[DW].[CLM_REMIT] where [CLM_FKEY] IN (%s)'
remit_adj_q = 'SELECT [CLM_REMIT_FKEY],[CLM_REMIT_SRVC_LN_FKEY],[CLM_ADJ_RSN_CD_FKEY],[CLM_ADJ_GRP_CD_FKEY],[ADJ_AMT],[ADJ_QTY] from [RI_BonSecours].[DW].[CLM_REMIT_ADJ] where [CLM_REMIT_FKEY] IN (%s)'
remit_rmk_q = 'SELECT [CLM_REMIT_FKEY],[CLM_REMIT_SRVC_LN_FKEY],[RMRK_CD_FKEY],[RMRK_SGMT] from [RI_BonSecours].[DW].[CLM_REMIT_RMRKS] where [CLM_REMIT_FKEY] IN (%s)'
remit_srvc_q = 'SELECT [CLM_REMIT_FKEY],[SRVC_CD],[SRVC_STRT_DT],[SRVC_END_DT],[SRVC_CD_SRC],[REV_CD_FKEY],[LN_ITEM_SBMT_AMT],[LN_ITEM_PYMNT_AMNT],[SRVC_UNIT_CNT_ORIG],[SRVC_UNIT_CNT_PAID] from [RI_BonSecours].[DW].[CLM_REMIT_SRVC_LN] where [CLM_REMIT_FKEY] IN (%s)'
clm_q = 'SELECT [CLM_PKEY], [PATNT_CNTRL_NBR], [PAYEE_ID], [PAYEE_NM], [PATNT_MED_RCRD_NBR],[PATNT_BRTH_DT],[PATNT_ZIP_CD_FKEY],[PATNT_QLFR_CD],[TYP_OF_BILL],[CFIC_CD],[CLM_ADMIT_DT],[CLM_DSCHRG_DT] from [RI_BonSecours].[DW].[CLM] where [CLM_PKEY] IN (%s)'
clm_trns_q = 'SELECT [CLM_FKEY],[CLM_TRNS_PKEY],[TRNSCTN_SET_CRTN_DT],[PAYER_RSPSBLTY_SEQ_NBR_CD],[SUBSCRBR_BRTH_DT],[SUBSCRBR_GNDR],[SUBSCRBR_QLFR_CD],[SRVC_AUTHRZTN_EXCPTN_CD],[RFRRL_NBR],[PRIOR_AUTHRZTN_NBR],[CLM_TOT_CHRG_AMT],[CLM_TRNS_ADMIT_DT],[ADMSSN_TYP_CD],[ADMSSN_SRC_CD],[PATNT_STTS_CD],[CLM_TRNS_DRG_CD_FKEY] from [RI_BonSecours].[DW].[CLM_TRNS] where [CLM_FKEY] IN (%s)'


claim_transaction_key_file = 'D:\\BonSecours\\3weeks\\db_dump\\data\\final_199K_claim_trans_keys.csv'
claim_pkey_file = 'D:\\BonSecours\\3weeks\\db_dump\\data\\final_79K_claim_keys.csv'
claim_remit_key_file = 'D:\\BonSecours\\3weeks\\db_dump\\data\\final_166K_claim_remit_keys.csv'

cnxn = pyodbc.connect(con_string)
cursor = cnxn.cursor()

key_filename = claim_pkey_file




fp = open("D:\\BonSecours\\3weeks\\db_dump\\new_data\\claim_remit_query_result.txt",'w')


myfile = csv.writer(fp,lineterminator='\n',delimiter = '\t', escapechar=' ', quoting=csv.QUOTE_NONE)

active_q = remit_q

head_line = active_q[active_q.find('['):active_q.rfind('from')]
myfile.writerow([head_line.replace('[','').replace(']','')])

def groupquery(listele):
    pl = ', '.join("'"+str(key).replace('\n','')+"'" for key in listele)
    query = active_q % pl
    return query

count = 0
with open(key_filename, 'r') as infile:
    lines = []
    for line in infile:
        lines.append(line)
        if len(lines) >= batchsize:
            count = count + batchsize
            print (count)
            exesql = groupquery(lines)
            #print (exesql)
            cursor.execute(exesql)
            rows = cursor.fetchall()
            for row in rows:
                myfile.writerow(row)

            lines = []
    if len(lines) > 0:
        exesql = groupquery(lines)
        print (count+len(lines))
        cursor.execute(exesql)
        rows = cursor.fetchall()
        for row in rows:
            myfile.writerow(row)


# csvreader = csv.reader(open("D:\\BonSecours\\3weeks\\db_dump\\107k_clm_trns_keys.csv"))
# next(csvreader,None)
#
#
# # write the headers
# header ='CLM_TRNS_FKEY,DIAG_CD_FKEY'
# myfile.writerow([header])
# count = 0
#
#
# for line in csvreader:
#     query = "select [CLM_TRNS_FKEY],[DIAG_CD_FKEY] from [DW].[CLM_TRNS_DIAG] WHERE [CLM_TRNS_FKEY] = '%s'"
#     count= count+1
#     print (count)
#     cursor.execute(query % line[0])
#
#     #print (cursor.query)
#     rows = cursor.fetchall()
#     for row in rows:
#         myfile.writerow(row)
#
cnxn.close()
fp.close()