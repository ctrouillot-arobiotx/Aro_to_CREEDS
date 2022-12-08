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


def calc_jaccard_index_df(df1, df2, CREEDS_jaccard_overlap):
    """"This function calculates the Jaccard Index when given a dataframe.
    It assumes that the dataframe includes genes as "SYMBOL", disease_records in numbers, and
    a column that will be used for comparison. It also assumes there's a DE genes column.
    returns a dataframe that includes the first_disease_record, the second_disease_record and the jaccard_index."""

    import numpy as np

    jaccard_current_set = set([str(fd) + ',' + str(sd) for fd, sd in
              zip(CREEDS_jaccard_overlap['first_disease_record'], CREEDS_jaccard_overlap['second_disease_record'])])

    #TODO: Switch this to record; the records are unique
    first_disease_records = df1['disease_record'].unique()
    second_disease_records = df2['disease_record'].unique()

    jaccard_index_summary_list = []
    jaccard_index_summary_list_unique = []

    for i in range(0, len(first_disease_records)):
        # get first unique disease in the disease record
        first_disease_index = first_disease_records[i]

        # get the unique list of gene symbols associated with the disease
        df = df1.loc[df1['disease_record'] == first_disease_index]
        list1 = df['SYMBOL'].astype(str) + '_' + df['DE_up'].astype(str)
        list1_unique = np.unique(list1)

        if first_disease_index < max(CREEDS_jaccard_overlap['first_disease_record']):
        # if first_disease_index < new values to be added to the database, use j from 0 to length of disease record

            for j in range(0,len(second_disease_records)):
                # export the rows that match the first_disease + second_disease
                second_disease_index = second_disease_records[j]


                #if it already exists in the current/old jaccard_index dataframe, skip
                y = str(first_disease_index) + ',' + str(second_disease_index)
                y2 = str(second_disease_index) + ',' + str(first_disease_index)

                #if y < CREEDS_jaccard_overlap['first_disease_record']

                if y in jaccard_current_set or y2 in jaccard_current_set:
                    pass
                #elif ##add in if we've already calculated it in the second jaccard matr
                else:
                    # set up the lists that will be passed into the jac index function
                    otherdf = df2.loc[df2['record'] == second_disease_index]
                    list2 = otherdf['SYMBOL'].astype(str) + '_' + otherdf['DE_up'].astype(str)
                    list2_unique = np.unique(list2)

                    jac_index_unique = calc_jaccard_index(list1_unique,list2_unique)

                    jaccard_index_summary_list.append(
                        create_jaccard_dataframe(first_disease_index, second_disease_index, jac_index_unique))
        else:
            #use j from first_disease_index to end
            for j in range(i,len(second_disease_records)):
                second_disease_index = second_disease_records[j]
                otherdf = df2.loc[df2['record'] == second_disease_index]
                list2 = otherdf['SYMBOL'].astype(str) + '_' + otherdf['DE_up'].astype(str)
                list2_unique = np.unique(list2)

                jac_index_unique = calc_jaccard_index(list1_unique, list2_unique)

                jaccard_index_summary_list.append(
                    create_jaccard_dataframe(first_disease_index, second_disease_index, jac_index_unique))

    if not jaccard_index_summary_list:
        pass
    else:
        jaccard_index_summary = pd.concat(jaccard_index_summary_list, ignore_index=True)
        return jaccard_index_summary

