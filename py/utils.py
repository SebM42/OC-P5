# Standard library
from datetime import datetime

# Third-party libraries
from pandas import DataFrame
from pandas import notnull
from pandas import to_datetime
from pymongo import MongoClient

# Local/project modules


def does_database_exist(mongo_client:MongoClient, database:str):
    return database in mongo_client.list_database_names()


def does_view_exists(mongo_client:MongoClient, database:str, view_name:str):
    views_list = [i["name"] for i in mongo_client[database].list_collections(filter={"type": "view"})]
    return view_name in views_list

def check_command_result(command_result:dict, command_name:str):
    if command_result['ok'] == 1:
        return '{} successfully\n'.format(command_name)
    else:
        raise Exception('Something weird happened during command \"{}\" ! Full response: {}').format(command_name, command_result)


def convert_columns_to_datatypes(df:DataFrame, target_data_types:dict):
    new_df = df.copy()
    columns = df.columns
    
    for col in columns:
        match target_data_types[col]: # noqa
            case 'string':
                new_df[col] = new_df[col].astype('object').fillna('')
                new_df[col] = new_df[col].astype('string')
            case 'int':
                s = new_df[col].astype('object')
                new_df[col] = s.where(notnull(s), None).astype('int')
            case 'float':
                new_df[col] = new_df[col].astype('float')
            case 'datetime':
                new_df[col] = to_datetime(new_df[col], errors='coerce')
            case 'object':
                new_df[col] = new_df[col].astype('object')
            case _:
                raise Exception('{target_data_types[col]} not recognized as datatype, check in config.py your COLUMNS_DTYPE_TARGET values')
    
    return new_df

mongo_pipeline_count_types_per_field = [
  {
    '$project': {
      'kv': { '$objectToArray': "$$ROOT" }
    }
  },
  { '$unwind': "$kv" },
  {
    '$project': {
      'field': "$kv.k",
      'type': { '$type': "$kv.v" }
    }
  },
  {
    '$group': {
      '_id': { 'field': "$field", 'type': "$type" },
      'count': { '$sum': 1 }
    }
  },
  {
    '$group': {
      '_id': "$_id.field",
      'types': {
        '$push': {
          'type': "$_id.type",
          'count': "$count"
        }
      }
    }
  },
  { '$sort': { "_id": 1 } }
]