## basically all functions related to pulling information out of and adding information into the database
import pandas as pd
import mysql.connector
import numpy as np
from sys import exit
#from functions_v2 import create_spreadsheet
#from jaccard_functions import calc_jaccard_index_df



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

#query functions
def create_CREEDS_metadata_df(disease, cell_type, do_id, organism, pert_ids, annotation_method):
    import pandas as pd

    data = {'disease': disease,
            'cell_type': cell_type,
            'do_id': do_id,
            'organism': organism,
            'pert_ids': pert_ids,
            'annotation_method': annotation_method}
    CREEDS_metadata_df = pd.DataFrame(data, index=[0])
    return CREEDS_metadata_df

def create_CREEDS_expression_df(SYMBOL, DE_up, disease_record):
    #disease record will be absolutely needed. okay, we'll run the CREEDS_metadata table and then insert that into MySQl
    #then we'll import the CREEDS_metadata to create the new CREEDS_expression dataframe
    #I'm not 100% sure how I'm going to mak eit so that it doesn't just repeat information that's already there
        #maybe import CREEDS metadata + CREEDS expression and wherever CREEDS expression ends, that's where we start?
    import pandas as pd
    #print(SYMBOL)
    #print(DE_up)
    #print(disease_record)
    data = {'SYMBOL': SYMBOL,
            'DE_up': DE_up,
            'disease_record': disease_record}
    CREEDS_expression_df = pd.DataFrame(data)
    return CREEDS_expression_df




def reformat_public_pompe_dataset(public_disease):
    """
    This function reformats the public_pompe_expression table. It assumes that the table includes
    logFC, model, P.Value, comparison, tissue, and dataset
    :param public_disease: dataframe that contains the columns mentioned above
    :return:
    """
    DISEASE_expression_list = []
    for i in range(0, public_disease.shape[0]):
        if public_disease['logFC'][i] > 0:
            DE_up = 1
        else:
            DE_up = 0

        if not public_disease.loc[i, 'model']:
            if public_disease.loc[i, 'SYMBOL'].isupper():
                public_disease.loc[i, 'model'] = 'human'
            else:
                public_disease.loc[i, 'model'] = 'mouse'

        DISEASE_expression_list.append(pd.DataFrame({'SYMBOL': public_disease['SYMBOL'][i],
                                                     'DE_up': DE_up,
                                                     'P.Value': public_disease['P.Value'][i],
                                                     'logFC': public_disease['logFC'][i],
                                                     'model': public_disease['model'][i],
                                                     'tissue': public_disease['tissue'][i],
                                                     'dataset': public_disease['dataset'][i],
                                                     'disease': public_disease['comparison'][i] + '_' +
                                                                public_disease['tissue'][i] + '_' +
                                                                public_disease['dataset'][i] + '_' +
                                                                public_disease['model'][i]}, index=[0]))
                                                    #note to self: double check that "disease" reflects the CREEDS dataset"
    DISEASE_expression_public = pd.concat(DISEASE_expression_list, ignore_index=True)
    return DISEASE_expression_public


def create_CREEDS_metadata(DISEASE_expression_public):
    import pandas as pd

    #Step 0:

    # Step 1: get unique list of diseases
    diseases = DISEASE_expression_public['disease']
    unique_diseases = diseases.unique()

    # Step 2: iterate through list of unique diseases
    CREEDS_UPDATE_metadata_list = []

    for disease in unique_diseases:
        # create CREEDS_metadata table
        cell_type = DISEASE_expression_public.loc[DISEASE_expression_public['disease'] == disease, 'tissue'].unique()
        do_id = DISEASE_expression_public.loc[DISEASE_expression_public['disease'] == disease, 'dataset'].unique()
        organism = DISEASE_expression_public.loc[DISEASE_expression_public['disease'] == disease, 'model'].unique()
        pert_ids = DISEASE_expression_public.loc[DISEASE_expression_public['disease'] == disease, 'dataset'].unique()
        annotation_method = 'ARO'

        CREEDS_UPDATE_metadata_list.append(
            create_CREEDS_metadata_df(disease, cell_type, do_id, organism, pert_ids, annotation_method))

    CREEDS_UPDATE_metadata = pd.concat(CREEDS_UPDATE_metadata_list, ignore_index=True)

    return CREEDS_UPDATE_metadata


def create_CREEDS_expression(DISEASE_expression_public):

    # what i could do
        #import CREEDS_metadata
        #iterate through disease record
        #use that disease record to get all of the associated genes and whether DE_up or not


    #Import current expression file
    CREEDS_expression = pd.read_sql("SELECT * FROM CREEDS_expression "
                                  "ORDER BY disease_record "
                                  "DESC LIMIT 1", con=cnx_TARGETID)

    # determine current highest disease record
    LAST_DISEASE_RECORD = CREEDS_expression['disease_record']

    # Step 0: get unique list of diseases
    diseases = DISEASE_expression_public['disease']
    unique_diseases = diseases.unique()

    # Step 1: iterate through list of unique diseases
    CREEDS_UPDATE_expression_list = []
    disease_record = LAST_DISEASE_RECORD

    for disease in unique_diseases:
        # create CREEDS_expression table
        disease_expression_subsection = DISEASE_expression_public.query('disease == @disease')
        CREEDS_UPDATE_expression_list.append(
            create_CREEDS_expression_df(disease_expression_subsection['SYMBOL'], disease_expression_subsection['DE_up'],
                                        disease_record))
        disease_record = disease_record + 1
    CREEDS_UPDATE_expression = pd.concat(CREEDS_UPDATE_expression_list, ignore_index=True)

    return CREEDS_UPDATE_expression

def into_CREEDS_metadata_db( disease_df):
    for i in range(0,len(disease_df['disease'])):
        sql = "INSERT INTO CREEDS_metadata SET \
                disease='" + str(disease_df['disease'][i]) + "', \
                cell_type='" + str(disease_df['cell_type'][i]) + "', \
                do_id='" + str(disease_df['do_id'][i]) + "', \
                organism='" + str(disease_df['organism'][i]) + "', \
                pert_ids='" + str(disease_df['pert_ids'][i]) + "', \
                annotation_method='" + str(disease_df['annotation_method'][i] + "'")

        # run the command in mysql
        cursor_TARGETID.execute(sql)

        # Commit the changes to the database
        cnx_TARGETID.commit()


def into_CREEDS_expression_db(disease_df):
    for i in range(0, len(disease_df['disease'])):
        sql = "INSERT INTO CREEDS_expression SET \
                SYMBOL='" + str(disease_df['SYMBOL'][i]) + "', \
                DE_up='" + str(disease_df['DE_up'][i]) + "', \
                disease_record='" + str(disease_df['record'][i]) + "' \
                ON DUPLICATE KEY UPDATE \
                SYMBOL='" + str(disease_df['SYMBOL'][i]) + "', \
                DE_up='" + str(disease_df['DE_up'][i]) + "', \
                disease_record='" + str(disease_df['record'][i]) + "'"

        # run the command in mysql
        cursor_TARGETID.execute(sql)

        # Commit the changes to the database
        cnx_TARGETID.commit()

        #print(sql)

def into_CREEDS_jaccard_overlap_db(jaccard_df):
    for i in range(0,jaccard_df.shape[0]):
        #TODO: Double check if there should be a "ON DUPLICATE KEY UPDATE"
        sql = "INSERT INTO CREEDS_jaccard_overlap SET \
                first_disease_record='" + str(jaccard_df['first_disease_record'][i]) + "', \
                second_disease_record='" + str(jaccard_df['second_disease_record'][i]) + "', \
                jaccard_index='" + str(jaccard_df['jaccard_index'][i]) + "' \
                ON DUPLICATE KEY UPDATE  \
                first_disease_record='" + str(jaccard_df['first_disease_record'][i]) + "', \
                second_disease_record='" + str(jaccard_df['second_disease_record'][i]) + "', \
                jaccard_index='" + str(jaccard_df['jaccard_index'][i]) + "'"



        # run the command in mysql
        print(sql)
        cursor_TARGETID.execute(sql)

        # Commit the changes to the database
        cnx_TARGETID.commit()

        #print(sql)
