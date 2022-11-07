## This function includes all of the files necessary to create the Jaccard Index
import pandas as pd

# Calculate Jaccard Index
def calc_jaccard_index(list1, list2):
    """This function can be used to calculate the Jaccard index of any 2 given lists
     :param list1 is the first list
     :param list2 is the second list.
     :return returns the Jaccard Index"""

    intersection = len(list(set(list1).intersection(list2)))
    union = (len(list1) + len(list2)) - intersection
    return float(intersection) / union



# Create Jaccard_Table:
def create_jaccard_dataframe(disease1, disease2, jac_index):
    """This function creates the dataframe for the jaccard index
     :param disease1 is the first disease record
     :param disease2 is the second disease record
     :param jac_index is the jaccard index of the two disease records
     :param values being compared
     :return returns the dataframe for the jaccard index"""


    data = {'first_disease_record': disease1,
            'second_disease_record': disease2,
            'jaccard_index': jac_index}

    jaccard_indexdf = pd.DataFrame(data, index=[0])
    return jaccard_indexdf


def calc_jaccard_index_df(df1, df2, comp_col):
    #TODO: make it so that you can choose the comparison column for the inserted dataframe
    #TODO: make the second dataframe optional and have it so that it can calculate the index for either 2 unique DFs or the same
    """"This function calculates the Jaccard Index when given a dataframe.
    It assumes that the dataframe includes genes as "SYMBOL", disease_records in numbers, and
    a column that will be used for comparison. It also assumes there's a DE genes column.
    returns a dataframe that includes the first_disease_record, the second_disease_record and the jaccard_index."""

    import numpy as np


    first_disease_record = df1['disease'].unique()
    second_disease_record = df2['disease'].unique()

    jaccard_index_summary_list = []
    jaccard_index_summary_list_unique = []

    for i in range(0, len(first_disease_record)):
        # get first unique disease in the disease record
        first_disease = first_disease_record[i]

        # get the disease_record of the first disease
        index1 = df1.loc[df1['disease'] == first_disease, 'disease_record'].unique()

        # get the unique list of gene symbols associated with the disease
        df = df1.loc[df1['disease'] == first_disease]
        list1 = df['SYMBOL'].astype(str) + '_' + df['DE_up'].astype(str)
        list1_unique = np.unique(list1)

        for j in range(i,len(second_disease_record)):
            # export the rows that match the first_disease + second_disease
            second_disease = second_disease_record[j]

            # get the disease_record of the second disease
            index2 = df2.loc[df1['disease'] == second_disease, 'disease_record'].unique()

            otherdf = df2.loc[df2['disease'] == second_disease]

            # set up the lists that will be passed into the jac index function
            list2 = otherdf['SYMBOL'].astype(str) + '_' + otherdf['DE_up'].astype(str)
            list2_unique = np.unique(list2)

            jac_index_unique = calc_jaccard_index(list1_unique,list2_unique)

            jaccard_index_summary_list.append(
                create_jaccard_dataframe(index1, index2, jac_index_unique))

    jaccard_index_summary = pd.concat(jaccard_index_summary_list, ignore_index=True)

    return jaccard_index_summary

