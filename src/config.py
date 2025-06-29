import os
# DB CONNECTION

USER_NAME = os.getenv('MONGO_ROOT_USERNAME')
PW = os.getenv('MONGO_ROOT_PASSWORD')
HOST = 'mongodb'
PORT = '27017'

DB_DRIVER_STRING = f'mongodb://{USER_NAME}:{PW}@{HOST}:{PORT}/'

# DATA RELATIVE PATH
FILE_DATA = 'data//healthcare_dataset.csv'

# MIGRATION
NEW_DB_NAME = 'healthcare'
NEW_MAIN_COLLECTION_NAME = 'admissions'
FLATTEN_VIEW_NAME = 'flatten_view'

COLUMNS_DTYPE_TARGET = {
    'name':'string',
    'age':'int',
    'gender':'string',
    'blood_type':'string',
    'medical_condition':'string',
    'date_of_admission':'datetime',
    'doctor':'string',
    'hospital':'string',
    'insurance_provider':'string',
    'billing_amount':'float',
    'room_number':'int',
    'admission_type':'string',
    'discharge_date':'datetime',
    'medication':'string',
    'test_results':'string'
}

MONGO_PY_DATATYPE_MATCHUP = {
    'string':'str',
    'int':'int',
    'double':'float',
    'date':'Timestamp'
}

COLUMS_TO_NORMALISE = [
    {   
     'names':'hospital', # name of the column to normalise in target dataset
     'new_column_name':'ref_hospital', # name of the new column containing foreign keys in target dataset
     'new_table_name':'hospitals' # name of the new dataset containing normalised column unique values
    }
]

COLUMNS_TO_SERIALISE = [
    {
     'names':['name','gender','blood_type'], # list of column to serialise into an object
     'new_column_name':'patient' # name of the new column containing serialised values
    }
]

# ADMIN AND ROLES

ROLES = [
    {
        'role': "customRead",
        'privileges': [
            {
                'resource': {'db': NEW_DB_NAME, 'collection': ""},
                'actions': ["find"]
            }
        ]
    },
    {
        'role': "customReadWrite",
        'privileges': [
            {
                'resource': {'db': NEW_DB_NAME, 'collection': ""},
                'actions': ["find", "insert", "update"]
            }
        ]
    },
    {
        'role': "customFull",
        'privileges': [
            {
                'resource': {'db': NEW_DB_NAME, 'collection': ""},
                'actions': ["find", "insert", "update", "remove"]
            }
        ]
    }
]




