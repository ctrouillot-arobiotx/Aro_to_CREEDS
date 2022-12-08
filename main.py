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
#public_disease = pd.read_sql("SELECT * FROM public_pompe_expression_data "
#                             "WHERE ABS(logFC) >1.0"
#                             "AND `P.Value` <0.05"
#                             , con=cnx_AROBIO)



# This actually creates an intermediary DataFrame with all the information that will be needed to create
# an updated CREEDS_metadata as well as CREEDS_expression dataframe
#DISEASE_expression_public = db_query_func.reformat_public_pompe_dataset(public_disease)

# Create a dataframe that mimics the CREEDS_metadata + CREEDS_expression dataframe
#CREEDS_UPDATE_metadata = db_query_func.create_CREEDS_metadata(DISEASE_expression_public)

# Insert new diseases into CREEDS_metadata
#db_query_func.into_CREEDS_metadata_db(CREEDS_UPDATE_metadata)

# Import new CREEDS_metadata table
#CREEDS_NEW_metadata = pd.read_sql("SELECT * FROM CREEDS_metadata ", con=cnx_TARGETID)

# Merge CREEDS_NEW_metadata & DISEASE_expression_public to have the
#result = pd.merge(CREEDS_NEW_metadata, DISEASE_expression_public, on="disease")
#print(result['record'])

# Using the result column, add the information into CREEDS_expression table
#db_query_func.into_CREEDS_expression_db(result)


# import the new CREEDS_expression + metadata
CREEDS_combined_mdexp = pd.read_sql("""SELECT * FROM CREEDS_expression 
                                       INNER JOIN CREEDS_metadata 
                                       ON CREEDS_expression.disease_record = CREEDS_metadata.record""",
                                    con = cnx_TARGETID)

# import jaccard_index (for comparison purposes)
CREEDS_jaccard_overlap = pd.read_sql("""SELECT * FROM CREEDS_jaccard_overlap""", con = cnx_TARGETID)


# calculate jaccard indices
jaccard = calculation_func.calc_jaccard_index_df(CREEDS_combined_mdexp,CREEDS_combined_mdexp,CREEDS_jaccard_overlap)
db_query_func.into_CREEDS_jaccard_overlap_db(jaccard)
print(jaccard)

