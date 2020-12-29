from models import db_connect,create_table

import pandas as pd
# from pymongo import MongoClient

engine = db_connect()
create_table(engine)
# mongo_uri='mongodb://root:password@localhost:27017'

def return_no_processed_df(table_name, pro_table_name, engine):
    '''
    compare with two table extract such row has not processed
    '''
    # client = MongoClient(mongo_uri)
    # db = client['petroleum_news']
    # table_db = db[table_name].find()
    ori_df = pd.read_sql_table(table_name,engine)
    # ori_df.index.name='orig_id'
    pro_df = pd.read_sql_table(pro_table_name, engine, index_col='id')
    
#     print(orid_df.head(),pro_df.head())
    ## column id has been processed
    id_list = pro_df.orig_id.values.tolist()

    # # if len(ori_df) == len(pro_df)
    # if len(id_list) == 0:
    #     return ori_df
    # else:
#     print(id_list)
    return ori_df[~(ori_df['id'].isin(id_list))]