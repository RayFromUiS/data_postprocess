from models import db_connect,create_table

import pandas as pd


engine = db_connect()
create_table(engine)


def return_no_processed_df(table_name, pro_table_name, engine):
    '''
    compare with two table extract such row has not processed
    '''
    ori_df = pd.read_sql_table(table_name, engine)
    pro_df = pd.read_sql_table(pro_table_name, engine, index_col='id')

    ## column id has been processed
    id_list = pro_df.orig_id.values.tolist()

    # # if len(ori_df) == len(pro_df)
    # if len(id_list) == 0:
    #     return ori_df
    # else:
    return ori_df[~ori_df['id'].isin(id_list)]