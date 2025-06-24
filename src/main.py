# Standard library

# Third-party libraries
from pandas import DataFrame
from pandas import read_csv
from pymongo import MongoClient

# Local/project modules
import config
from utils import check_command_result
from utils import convert_columns_to_datatypes
from utils import does_database_exist
from utils import does_view_exists



def load_data(file:str):
    print(f'Loading {file} ...')
    data_csv = read_csv(file)
    print('File succesfully loaded\n')
    return data_csv


def standardise_columns_names(df:DataFrame):
    return df.rename(columns={col:col.lower().replace(' ','_') for col in df.columns})


def schema_normalise_columns(data:DataFrame, column_to_normalize:str|list, new_column_name:str):
    # extract all unique values from column_name into a separate table, assigning each a unique ID
    # and replace the original column’s values with those IDs
    # returns both dataframe of uniques from column_name and data with replaced values
    uniques_from_column_name = data[column_to_normalize].copy().drop_duplicates().reset_index(drop=True).reset_index().rename(columns={'index':new_column_name})
    new_data = data.merge(uniques_from_column_name,on=column_to_normalize,how='inner').drop(column_to_normalize, axis=1)
    uniques_from_column_name = uniques_from_column_name.rename(columns={new_column_name:'_id'})
    return uniques_from_column_name, new_data


def json_serialisation(data:DataFrame, columns:str|list, json_column_name:str):
    # serialise a list of column into json format and put them in a new column named after json_column_name
    # then removes the list of columns from the dataframe
    new_data = data.copy()
    new_data[json_column_name] = new_data[columns].apply(lambda x: x.to_dict(), axis=1)
    new_data = new_data.drop(columns, axis=1)
    return new_data


def insert_data_in_new_mongodb(driver_string:str, db_name:str, collections:dict):
    # connect to the db
    mc = MongoClient(driver_string)
    
    # check if db doest exist and create a new one else abort
    if does_database_exist(mc, db_name):
        raise Exception("Already a database named {}, operation aborted".format(db_name))  
    else:
        db = mc[db_name]
    
    ids = {} # dictionnary of created collection ID lists
    for collection_name, collection in zip(collections.keys(), collections.values()):
        print(f'Inserting {collection_name} into {db_name} database ...')
        ids[collection_name] = db[collection_name].insert_many(collection.to_dict(orient='records'))
        inserted_entries = len(ids[collection_name].inserted_ids)
        if inserted_entries > 0:
            print(f"Collection {collection_name} succesfully inserted with {inserted_entries} entries\n")
        else:
            print(f"Collection {collection_name} is empty : {inserted_entries} entries inserted\n")
    
    return mc, ids


def create_flattening_pipeline(normalised:list, serialised:list):
    pipeline = []
    addfields = {}
    project = {'_id':0}
    
    # for each normalisation operation, we create a lookup with the new table created
    for i in normalised:
        lookup = {'from':i['new_table_name'],
                  'localField':i['new_column_name'],
                  'foreignField':'_id',
                  'as':i['new_table_name']+'Docs'
                 }
        pipeline.append({'$lookup':lookup})
        # and unwind it to show each field as columns
        unwind = {'$unwind':'$'+i['new_table_name']+'Docs'}
        pipeline.append(unwind)
        
        # then we remove from the view the field on which the lookup was made and the container field
        project[i['new_table_name']+'Docs'] = 0
        project[i['new_column_name']] = 0
        
        # for each field in the looked up collection, we add them to the view
        if isinstance(i['names'],list):
            for col in i['names']:
                addfields[col] = '$'+i['new_table_name']+'Docs.'+col
        else:
            addfields[i['names']] = '$'+i['new_table_name']+'Docs.'+i['names']
    
    # for each serialisation operation, we add serialised fields to the view and remove the container field
    for j in serialised:
        if isinstance(j['names'],list):
            for name in j['names']:
                addfields[name] = '$'+j['new_column_name']+'.'+name
        else:
            addfields[j['names']] = '$'+j['new_column_name']+'.'+j['names']
        
        project[j['new_column_name']] = 0
    
    if len(addfields) > 0:
        pipeline.append({'$addFields':addfields})
    pipeline.append({'$project':project})
    
    return pipeline


def create_flatten_view(mongo_client:MongoClient, database:str, view_name:str, view_on:str, normalised:list, serialised:list, overwrite:bool = False):
    # check if database exists
    if does_database_exist(mongo_client, database) == False:
        raise Exception("No database named {}, operation aborted".format(database))  
    else:
        db = mongo_client[database]
    
    # generate the pipeline
    pipeline = create_flattening_pipeline(normalised, serialised)
    
    # check if view exists and we have overwrite permission
    if does_view_exists(mongo_client,database,view_name):
        if overwrite:
            print('Editing existing flatten view ...')
            command_result = db.command({"collMod":view_name, "viewOn":view_on, "pipeline":pipeline})
            print(check_command_result(command_result,'View edited'))
        else:
            raise Exception("A view named {} already exists, operation aborted".format(view_name))  
    else:
        print('Creating flatten view ...')
        command_result = db.command('create', view_name, viewOn=view_on, pipeline=pipeline)
        print(check_command_result(command_result,'View created'))
  

def create_roles(mongo_client:MongoClient, roles_config:list):
    db = mongo_client['admin']
        
    # create roles
    for role in roles_config:
        command_result = db.command("createRole", role['role'], privileges=role['privileges'], roles=[])
        print(check_command_result(command_result,f"Role {role['role']} created"))
    
    

# MAIN
if __name__ == '__main__':
    data_original = load_data(config.FILE_DATA)

    data_standardised = standardise_columns_names(data_original)
    
    data_typed = convert_columns_to_datatypes(data_standardised, config.COLUMNS_DTYPE_TARGET)
    
    new_collections = {}
    data_normalised = data_typed.copy()
    for i in config.COLUMS_TO_NORMALISE:
        new_collections[i['new_table_name']], data_normalised = schema_normalise_columns(data_normalised, i['names'], i['new_column_name'])
    
    data_normalised = data_normalised.reset_index(names='_id')
    
    data_serialised = data_normalised.copy()
    for j in config.COLUMNS_TO_SERIALISE:
        data_serialised = json_serialisation(data_serialised, j['names'],j['new_column_name'])

    new_collections[config.NEW_MAIN_COLLECTION_NAME] = data_serialised
    
    
    
    
    mongo_client, table_ids = insert_data_in_new_mongodb(config.DB_DRIVER_STRING, config.NEW_DB_NAME, new_collections)
    
    create_flatten_view(mongo_client, config.NEW_DB_NAME, config.FLATTEN_VIEW_NAME, config.NEW_MAIN_COLLECTION_NAME,
                        config.COLUMS_TO_NORMALISE, config.COLUMNS_TO_SERIALISE)

    create_roles(mongo_client, config.ROLES)

    