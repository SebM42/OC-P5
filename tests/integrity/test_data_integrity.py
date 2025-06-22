# Standard library

# Third-party libraries
import pytest
from pymongo import MongoClient
from pandas import DataFrame
from pandas import json_normalize
from pandas import concat

# Local/project modules
from src.main import load_data
from src.main import standardise_columns_names
from src.main import convert_columns_to_datatypes
from src import config
from utils import does_database_exist
from utils import does_view_exists
from utils import mongo_pipeline_count_types_per_field


@pytest.fixture(scope="session")
def mongo_client():
    return MongoClient(config.DB_DRIVER_STRING)

@pytest.fixture(scope="session")
def database():
    return config.NEW_DB_NAME
    
@pytest.fixture(scope="session")
def flatten_view():
    return config.FLATTEN_VIEW_NAME

@pytest.fixture(scope="session")
def data_to_compare():
    data_original = load_data(config.FILE_DATA)

    data_standardised = standardise_columns_names(data_original)
    
    data_typed = convert_columns_to_datatypes(data_standardised, config.COLUMNS_DTYPE_TARGET)
    return data_typed


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
        mongo_types = json_normalize(mongo_types_exploded["types"])
        mongo_types = concat([mongo_types_exploded.drop(columns="types").reset_index(drop=True),mongo_types], axis=1)
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
    data_A_sorted_columns = data_A.columns.sort_values()
    data_A = data_A[data_A_sorted_columns].copy().sort_values(data_A_sorted_columns.tolist())
    data_B_sorted_columns = data_B.columns.sort_values()
    data_B = data_B[data_B_sorted_columns].copy().sort_values(data_B_sorted_columns.tolist())
    
    result = data_A.compare(data_B, align_axis=1)
    
    # ASSERT : result must be an empty dataframe = no difference
    
    assert result.shape == (0,0)