import config.py
from pymongo import MongoClient
from pandas import DataFrame
import pandas as pd

from utils import convert_columns_to_datatypes
from utils import does_database_exist
from utils import does_view_exists
from utils import mongo_pipeline_count_types_per_field

from main import load_data
from main import standardise_columns_names
from main import convert_columns_to_datatypes


def test_data_type_integrity(mongo_client:MongoClient, database:str, flatten_view:str, data_to_compare:DataFrame):
    # ARRANGE : get count of each datatype for all fields of the flatten_view and for all columns of data_to_compare
    # check if database exists
    if does_database_exist(mongo_client, database) == False:
        raise Exception("No database named {}, test aborted".format(database))  
    else:
        db = mongo_client[database]
    
    # get data from view if it exists
    if does_view_exists(mongo_client,database,flatten_view):
        data = DataFrame(list(db[flatten_view].aggregate(mongo_pipeline_count_types_per_field)))
        mongo_types_exploded = data.explode("types")
        mongo_types = pd.json_normalize(mongo_types_exploded["types"])
        mongo_types = pd.concat([mongo_types_exploded.drop(columns="types").reset_index(drop=True),mongo_types], axis=1)
        mongo_types = mongo_types.rename(columns={'_id':'col_name','type':'data_type'})
        # convert monto type to py type
        mongo_types['data_type'] = mongo_types['data_type'].apply(lambda x: config.MONGO_PY_DATATYPE_MATCHUP[x])
        
    else:
        raise Exception("No view named {}, test aborted".format(flatten_view))
    
    # get types count per column from data_to_compare
    data_types = data_to_compare.map(lambda x : type(x).__name__).stack().reset_index()
    data_types = data_types.rename(columns={'level_0':'id','level_1':'col_name', 0:'data_type'})
    data_types = data_types.groupby(['col_name','data_type'], sort=False, as_index=False).count().rename(columns={'id':'count'})

    # ACT : compare both dataset after aligning them
    mongo_types = mongo_types.sort_values('col_name').reset_index(drop=True)
    data_types = data_types.sort_values('col_name').reset_index(drop=True)
    
    result = mongo_types.compare(data_types, align_axis=1)
    
    # ASSERT : result must be an empty dataframe = no difference
    
    assert result.shape == (0,0)

def test_data_value_integrity(mongo_client:MongoClient, database:str, flatten_view:str, data_to_compare:DataFrame):
    # ARRANGE : get flatten data drom mongo database as data_A, and data_to_compare as data_B
    # check if database exists
    if does_database_exist(mongo_client, database) == False:
        raise Exception("No database named {}, test aborted".format(database))  
    else:
        db = mongo_client[database]
    
    # get data from view if it exists
    if does_view_exists(mongo_client,database,flatten_view):
        flatten_data = DataFrame(list(db[flatten_view].find()))
    else:
        raise Exception("No view named {}, test aborted".format(flatten_view))
    
    data_A = flatten_data
    data_B = data_to_compare
        
    
    # ACT : compare both datasets after aligning them
    # alignment
    data_A_sorted_columns = data_A.columns.sort_values()
    data_A = data_A[data_A_sorted_columns].copy().sort_values(data_A_sorted_columns.tolist())
    data_B_sorted_columns = data_B.columns.sort_values()
    data_B = data_B[data_B_sorted_columns].copy().sort_values(data_B_sorted_columns.tolist())
    
    result = data_A.compare(data_B, align_axis=1)
    
    # ASSERT : result must be an empty dataframe = no difference
    
    assert result.shape == (0,0)
    
# MAIN
if __name__ == '__main__':
    data_original = load_data(config.FILE_DATA)

    data_standardised = standardise_columns_names(data_original)
    
    data_typed = convert_columns_to_datatypes(data_standardised, config.COLUMNS_DTYPE_TARGET)
    
    mc = MongoClient(config.DB_DRIVER_STRING)
    db_name = config.NEW_DB_NAME
    flatten_view_name = config.FLATTEN_VIEW_NAME
    
    test_data_type_integrity(mc, db_name , flatten_view_name, data_typed)
    test_data_value_integrity(mc, db_name , flatten_view_name, data_typed)