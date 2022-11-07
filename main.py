# Main Function to Update CREEDS Database with Aro Information


#import python
import pandas as pd
import numpy as np
import mysql.connector

#import functions
from util import db_query_func
from util import calculation_func

#MySQL Connection
#global connection variables:
cnx_AROBIO = mysql.connector.connect(user='ctrouillot',
                              password='ctrouillotARO',
                              database='AROBIO',
                              host='10.2.35.19')

cursor_AROBIO = cnx_AROBIO.cursor()

cnx_TARGETID = mysql.connector.connect(user='ctrouillot',
                               password='ctrouillotARO',
                               database='TARGET_ID',
                               host='10.2.35.19')
cursor_TARGETID = cnx_TARGETID.cursor()

# Read in public data that meet the minimum log FC and maximum P-Value
public_disease = pd.read_sql("SELECT * FROM public_pompe_expression_data "
                             "WHERE ABS(logFC) >1.0"
                             "AND `P.Value` <0.05"
                             "LIMIT 500"
                             , con=cnx_AROBIO)



# This actually creates an intermediary DataFrame with all the information that will be needed to create
# an updated CREEDS_metadata as well as CREEDS_expression dataframe
DISEASE_expression_public = db_query_func.reformat_public_pompe_dataset(public_disease)


# Create a dataframe that mimics the CREEDS dataframe
CREEDS_UPDATE_metadata = db_query_func.create_CREEDS_metadata(DISEASE_expression_public)

# TODO: DELETE THE FOLLOWNG LINES OF CODE ONCE THE ADDITION TO MYSQL IS SETUP
disease_record=[]
for i in range(0,len(CREEDS_UPDATE_metadata)):
    disease_record.append(i)
CREEDS_UPDATE_metadata['disease_record'] = disease_record


# Insert new dataframe into CREEDS
#db_query_func.into_CREEDS_metadata_db(CREEDS_UPDATE_metadata)
#cursor.execute("INSERT INTO fruit (name, variety) VALUES (%s, %s)", (new_fruit, new_fruit_type));
# Pull out newly created dataframe

#for now merge the updated metadata and the expression data
result = pd.merge(CREEDS_UPDATE_metadata, DISEASE_expression_public, on="disease")



### this next section will insert the CREEDS_expression into the database

#Step 0: import the entirety of the CREEDS_expression int
#CREEDS_metadata = pd.read_sql("SELECT * FROM CREEDS_metadata ", con=cnx2)

CREEDS_expression = pd.read_sql("""SELECT * FROM CREEDS_expression""", con=cnx_TARGETID)
CREEDS_metadata = pd.read_sql("""SELECT * FROM CREEDS_metadata""", con=cnx_TARGETID)

OLD_LAST_DISEASE_RECORD = CREEDS_expression.loc[CREEDS_expression.shape[0]-1,'disease_record']
NEW_LAST_DISEASE_RECORD = CREEDS_metadata.loc[CREEDS_metadata.shape[0]-1,'record']


#for i in range(OLD_LAST_DISEASE_RECORD,NEW_LAST_DISEASE_RECORD):
    # i can just merge the metadata file with the DISEASE_Expression_Public file
    # from there i can just iterate through all of the numbers
    # pull out the gene name, DE_up, and disease record for each individual
    # create the disease expression table

db_query_func.into_CREEDS_expression_db(result)

# Insert CREEDS_expression into CREEDS_expression table

# calculate jaccard indices compared to what was currently in the dataframe

# Step 0: import the merged CREEDS_metadata and CREEDS_expression file

# Step 1: calculate the jaccard index of the ones already in there vs the new ones

# Step 2: calculate the jaccard index of the new ones vs the new ones
CREEDS_combined_mdexp = pd.read_sql("""SELECT * FROM CREEDS_expression 
                                       INNER JOIN CREEDS_metadata 
                                       ON CREEDS_expression.disease_record = CREEDS_metadata.record""",
                                    con = cnx_TARGETID)


#calculation_func.calc_jaccard_index_df(CREEDS_combined_mdexp.loc[0:OLD_DISEASE_RECORD,:],CREEDS_combined_mdexp[OLD_DISEASE_RECORD:NEW_DISEASE_RECORD,:)

#df_unique = calculation_func.calc_jaccard_index_df(result,result,'comp_col')
#print(df_unique)

