############################################################
##    Python script to convert Excel file to SQL query    ##
##                Created on 22-May-2023                  ##    
##  usage: place input Excel file in same dir of          ##
##         script, then run cmd 'python redact_oracle.py' ##       
############################################################

import pandas as pd
import os

### Read data from Excel file, sheet_name contain sheet ID start with 0
df = pd.read_excel('Profile Area XU.xlsx',sheet_name=[0])
#df = pd.read_excel('Summary Column List For Migrate Data20230606.xlsx',sheet_name=[0,1])

### Create array for name mapping
name_arr = ['AXU']
#name_arr = ['ODS','NEW20230606']

#########################
##      Start Loop     ##
#########################

for sheet_no in range(len(df)):
    
    ### get data from sheet and filter only PII columns
    print('----- Start '+name_arr[sheet_no]+' Tables ------')
    if sheet_no in range(0,3):
        df_raw = df[sheet_no][['OWNERNAME','TABLENAME','COLUMN_NAME']].loc[df[sheet_no]['PII_STATUS']=='PII'].copy()
        df_drop = df_raw[['OWNERNAME','TABLENAME']].copy()
        df_drop = df_drop.drop_duplicates()
    else:
        ### rename columns to be same as previous sheets
        df_raw = df[sheet_no][['OWNER','TABLE_NAME','COLUMN_NAME']].loc[df[sheet_no]['PII Status']=='PII'].copy()
        df_drop = df_raw[['OWNER','TABLE_NAME']].copy()
        df_drop = df_drop.drop_duplicates()
        df_raw = df_raw.rename(columns={'OWNER':'OWNERNAME', 'TABLE_NAME':'TABLENAME'})
        df_drop = df_drop.rename(columns={'OWNER':'OWNERNAME', 'TABLE_NAME':'TABLENAME'})
    #print(name_arr[sheet_no]+' Column LIST', df_raw)
    
    ### Create new ROWNUM column
    df_raw['ROWNUM'] = df_raw.groupby(['OWNERNAME','TABLENAME']).transform('rank')
    
    ### Sort column by ROWNUM to set the 1st row as add_policy and following rows as alter_policy
    df_final = df_raw.sort_values(['OWNERNAME','TABLENAME','ROWNUM'])
    #print(name_arr[sheet_no]+' Column LIST wit ROWNUM\n\n', df_final,'\n-------xxxxx--------\n')
    
    ### delete file if exist
    if not os.path.exists("redact_output"):
        os.mkdir("redact_output")
    if os.path.exists("redact_output\\"+name_arr[sheet_no]+"_sql.sql"):
        os.remove("redact_output\\"+name_arr[sheet_no]+"_sql.sql")
    if os.path.exists("redact_output\\"+name_arr[sheet_no]+"_sql_drop.sql"):
        os.remove("redact_output\\"+name_arr[sheet_no]+"_sql_drop.sql")
    
    ### Create output file
    output_file = open("redact_output\\"+name_arr[sheet_no]+'_sql.sql','w')
    output_file_drop = open("redact_output\\"+name_arr[sheet_no]+'_sql_drop.sql','w')
    
    ### Do looping to generate redaction SQL for all PII column
    for ind in df_final.index:
        my_sql = ''
        
        ### if be 1st column of the table then use add_policy 
        if df_final['ROWNUM'][ind] == 1:
    
            my_sql = "BEGIN\n \
\tDBMS_REDACT.ADD_POLICY(\n \
\t\tobject_schema => '"+df_final['OWNERNAME'][ind]+"',\n \
\t\tobject_name => '"+df_final['TABLENAME'][ind]+"',\n \
\t\tcolumn_name => '"+df_final['COLUMN_NAME'][ind]+"',\n \
\t\tpolicy_name => 'REDACT_POLICY',\n \
\t\tfunction_type => DBMS_REDACT.FULL,\n \
\t\texpression => '1=1'\n \
\t);\n \
END;\n \
/\n"
    
        ### if it isn't 1st column of the table then use alter_policy 
        else:
            #print(df_final['OWNERNAME'][ind],df_final['TABLENAME'][ind],df_final['COLUMN_NAME'][ind])
            my_sql = "BEGIN\n \
\tDBMS_REDACT.ALTER_POLICY(\n \
\t\tobject_schema => '"+df_final['OWNERNAME'][ind]+"',\n \
\t\tobject_name => '"+df_final['TABLENAME'][ind]+"',\n \
\t\tcolumn_name => '"+df_final['COLUMN_NAME'][ind]+"',\n \
\t\tpolicy_name => 'REDACT_POLICY',\n \
\t\tfunction_type => DBMS_REDACT.FULL,\n \
\t\texpression => '1=1'\n \
\t);\n \
END;\n \
/\n"
            #print(ind, my_sql)
        output_file.write(my_sql)
        
    ### end loop ###
    for ind in df_drop.index:
        my_sql_drop = ''
        my_sql_drop = "BEGIN\n \
\tDBMS_REDACT.DROP_POLICY(\n \
\t\tobject_schema => '"+df_drop['OWNERNAME'][ind]+"',\n \
\t\tobject_name => '"+df_drop['TABLENAME'][ind]+"',\n \
\t\tpolicy_name => 'REDACT_POLICY'\n \
\t);\n \
END;\n \
/\n"
        output_file_drop.write(my_sql_drop)
        
    output_file.close()
    output_file_drop.close()
    
    print('completed ', len(df_final), ' columns')
    print('------ End '+name_arr[sheet_no]+' Tables -------\n')

print('run script successfully ;)\n')
#########################
##      End Script     ##
#########################

