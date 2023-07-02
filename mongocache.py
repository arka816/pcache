'''
    TODO: disable cacheing on error; create disabler for that - DONE
    TODO: incrementer for timestamp conversion; implement using event loops or multithreading in a thread safe manner - incr_op_counter spread across all threads
    TODO: implement strict mode
    TODO: implement null handling
    TODO: error logging and warning logging
    TODO: make thread safe
    TODO: implement stale data definition: shelf_life
    TODO: implement all functionalities from cachier
    TODO: allow both remote and local cacheing
    TODO: allow user to define serializer
    TODO: enforce datatype (args, kwargs) and return types to be non-null
'''

import pymongo
import os
import datetime
import bson
import re
import json

from copy import deepcopy

from bson.decimal128 import Decimal128
from bson.timestamp import Timestamp

from utils import _coerce_decimal128, _coerce_float, _coerce_timestamp, _coerce_datetime, set_interval

'''
    The decimal128 BSON type uses the IEEE 754 decimal128 decimal-based floating-point numbering format. 
    Unlike binary-based floating-point formats such as the double BSON type, decimal128 does not approximate 
    decimal values and is able to provide the exact precision required for working with monetary data.
'''

INDEX_NAME = 'mongocache_index'

ATOMIC_BSON_TYPES = {
    int     : 'int',
    float   : 'decimal', 
    str     : 'string', 
    bool    : 'bool', 
    datetime.datetime   : 'date', 
    re.Pattern          : 'regex',
    bytes               : 'binary'
}

'''
    From pymongo docs:

    1.  A Python int will be saved as a BSON int32 or BSON int64 depending on its size. 
        A BSON int32 will always decode to a Python int. A BSON int64 will always decode to a Int64.
    2.  The bytes type is encoded as BSON binary with subtype 0. It will be decoded back to bytes.
    3.  Regex instances and regular expression objects from re.compile() are both saved as BSON regular expressions. 
        BSON regular expressions are decoded as Regex instances.
    4.  datetime.datetime instances will be rounded to the nearest millisecond when saved. the conversion is bidirectional.
    5.  cacheing requires high precision storage of floating point values. Decimal128 is the best choice among all BSON types.
        Decima128 supports comparison among NaN values and returns True since their BID representations are same.
'''

ATOMIC_BSON_CONVERTERS = {
    float   : _coerce_decimal128,
    datetime.datetime: _coerce_timestamp
}

ATOMIC_PYTHON_CONVERTERS = {
    Decimal128  : _coerce_float,
    Timestamp   : _coerce_datetime
}

BUILTIN_ITERABLES = frozenset([list, tuple, set, frozenset])


def mongocache(db_name, collection_name, port=27017, schema=None, strict=True):
    '''
        decorator factory callable that creates and returns the wrapper decorator
    '''
    # define all persistent variables here to avoid garbage collection in runtime
    client = None
    collection = None 
    key_names = []
    sentinel = object()
    enabled = True
    
    mongo_op_counter = 0

    # hit-miss statistics
    hits, misses, errors = 0, 0, 0

    def reset_op_counter():
        nonlocal mongo_op_counter
        mongo_op_counter = 0

    def incr_op_counter():
        nonlocal mongo_op_counter
        mongo_op_counter += 1
        return mongo_op_counter

    reset_task = set_interval(reset_op_counter, 1)

    # define utility to disable cache on error
    def disable_cache():
        nonlocal enabled, reset_task
        enabled = False
        reset_task.cancel()

    # test connection and check if collection exists from previous calls
    try:
        # create connection
        client = pymongo.MongoClient("localhost", port)
    except:
        # connection failed
        # log error and disable cacheing
        print("connection failed...cacheing disabled ")
        disable_cache()
    else:
        # create database if not exists
        db = client[db_name]

        # check if collection exists
        if collection_name in db.list_collection_names():
            collection = db[collection_name]
        
        if collection is None:
            # close connection...recreate connection lazily later when wrapped function is called 
            client.close()


    def wrapper(func):
        def wrapped_func(*args, **kwargs):
            nonlocal client, collection, key_names
            nonlocal port, db_name, collection_name, schema
            nonlocal hits, misses, errors, sentinel
            nonlocal enabled, disable_cache

            # TODO: make collection object creation thread safe
            # after they are created in a thread safe manner thread safety for all operations
            # on the collection object is implemented by mongoDB
            if enabled:
                # get list of arguments including kwargs sorted according to key
                all_args = args + tuple(dict(sorted(kwargs.items())).values())

                if collection is None:
                    '''on first function call create a collection if not already created'''

                    # create key names on first call
                    key_names = tuple([f"key_{i}" for i in range(len(all_args))])

                    # get function output to cache
                    data = func(*args, **kwargs)

                    # create schema from data and key_names if not given
                    if schema is None:
                        schema = _create_schema(data, key_names, all_args, disable_cache)

                    # create collection
                    client, collection = _create_collection(port, db_name, collection_name, key_names, schema, disable_cache)

                    print("created new collection")
                else:
                    # get key names from collection
                    key_names = _get_keynames(collection, disable_cache)


                # query cache
                document = _query(collection, key_names, all_args, disable_cache)                

                # update hit miss statistics
                if document is not None:
                    data = document.get('response', sentinel)
                    if data is not sentinel:
                        print("cache hit")
                        hits += 1
                    else:
                        errors += 1
                else:
                    ''' cache miss '''
                    data = None
                    misses += 1


                if data is None:
                    '''cache miss - call function and cache output'''
                    data = func(*args, **kwargs)
                    _push_data(collection, data, key_names, all_args, disable_cache)

                return data
            else:
                '''cache is disabled'''
                return func(*args, **kwargs)
        
        return wrapped_func
    return wrapper

def _get_keynames(collection, error_handler):
    index_info = collection.index_information().get(INDEX_NAME, None)

    if index_info is None:
        print("index not found...cacheing disabled")
        error_handler()
        key_names = None
    else:
        key_names = [key[0] for key in index_info['key']]
    
    return key_names

def _create_schema_recursive(data):
    '''
        Input:
            structured data

        Return:
            schema for input object
            keys:
            - bsonType of data
            - properties if bsonType is object or items if bsonType is array
    '''
    # check if atomic type
    if data is None:
        raise RuntimeError("null value encountered")

    if type(data) in ATOMIC_BSON_TYPES:
        bsonType = ATOMIC_BSON_TYPES[type(data)]
        return {'bsonType': bsonType}
    elif type(data) in BUILTIN_ITERABLES:
        # get bsonType for each item
        item_bson_types = [_create_schema_recursive(item) for item in data]

        # check if bson types are all the same
        bson_type_same = all(item_bson_type['bsonType'] == item_bson_types[0]['bsonType'] for item_bson_type in item_bson_types)
        if not bson_type_same:
            raise RuntimeError("items in array do not belong to the same BSON type")

        return {
            'bsonType'  : 'array',
            'items'     :  item_bson_types[0],
        }
    elif type(data) == dict:
        # get bson Type for each item
        properties = {key: _create_schema_recursive(val) for key, val in data.items()}

        return {
            'bsonType'  : 'object',
            'properties': properties
        }
    else:
        raise RuntimeError(f"could not convert data of type {type(data)} into known BSON types")
    
def _add_unique_clause(sub_schema):
    if sub_schema['bsonType'] == 'object':
        for key in sub_schema['properties']:
            if 'key'  in key:
                sub_schema['properties'][key] = _add_unique_clause(sub_schema['properties'][key])
    elif sub_schema['bsonType'] == 'array':
        sub_schema['items'] = _add_unique_clause(sub_schema['items'])
    else:
        sub_schema['unique'] = True
    return sub_schema

def _create_schema(data, key_names, args, error_handler):
    '''
        Input:
            data: sample data
            key_names: names of keys corresponging to inputs args and kwargs
            args: list of arguments to the cached function
        Return:
            schema: schema based on sample entry
    '''

    # construct entry
    entry = {}

    for key_name, arg in zip(key_names, args):
        entry[key_name] = arg

    entry['response'] = data

    # iterate through the entry recursively to get the schema
    # throw error when an entry does not match a BSON type
    try:
        schema = _create_schema_recursive(entry)
    except:
        print("error generating schema")
        error_handler()

    # schema for key_names must have an unique clause
    # for key in schema['properties']:
    #     if 'key'  in key:
    #         schema['properties'][key] = _add_unique_clause(schema['properties'][key])

    # add timestamp to schema
    schema['properties'].update({
        "timestamp": {
            "bsonType": "timestamp"
        }
    })

    return schema

def _create_collection(port, db_name, collection_name, key_names, schema, error_handler):
    '''
        Input:
            - port: port id
            - db_name: name of database
            - collection_name: name of collection
            - key_names: key names for creating index
            - schema: schema for collection

        Return:
            pymongo client (to avoid garbage collection)
            create a collection and return it
    '''
    try:
        # create database
        client = pymongo.MongoClient("localhost", port)
        db = client[db_name]

        print("created database")
        print(db.list_collection_names())

        # create collection
        if collection_name not in db.list_collection_names():
            db.create_collection(
                collection_name,
                validator = {
                    '$jsonSchema': schema
                }
            )
        collection = db[collection_name]

        print("created collection")

        # create compound index using cache keys and timestamp (for LRU implementation)
        # use E-S-R rule
        # cache keys use exact match
        # timestamp is only used for sorting
        indices = [(key, pymongo.ASCENDING) for key in key_names] + [("timestamp", pymongo.DESCENDING)]
        collection.create_index(indices, unique=True, name=INDEX_NAME)

        print("created index")
    except Exception as ex:
        # log error
        print(ex)
        error_handler()
        return None, None
    else:
        return client, collection

def _coerce_python_recursive(data):
    '''
        recursively iterate through the data structure
        and convert BSON data from queried document to pythonic builtins
    '''
    if type(data) in ATOMIC_PYTHON_CONVERTERS:
        return ATOMIC_PYTHON_CONVERTERS[type(data)](data)
    elif type(data) in BUILTIN_ITERABLES:
        return [_coerce_python_recursive(item) for item in data]
    elif type(data) == dict:
        for key, val in data.items():
            data[key] = _coerce_python_recursive(val)
        return data
    return data

def _process_query(data):
    '''
        coerces data into python builtins

        Input:
            cached data queried from collection

        Returns:
            cleaned data with coerced pythonic format
    '''

    data = _coerce_python_recursive(data)
    
    del data['_id'], data['timestamp']

    return data

def _coerce_bson_recursive(data):
    '''
        recursively iterate through the data structure
        and convert pythonic builtin data types to BSON format
    '''
    if type(data) in ATOMIC_BSON_CONVERTERS:
        return ATOMIC_BSON_CONVERTERS[type(data)](data)
    elif type(data) in BUILTIN_ITERABLES:
        return [_coerce_bson_recursive(item) for item in data]
    elif type(data) == dict:
        for key, val in data.items():
            data[key] = _coerce_bson_recursive(val)
        return data
    return data

def _coerce_bson(data):
    '''
        coerces data into one of the allowed BSON types

        Input:
            raw function output data to be cached

        Returns:
            data with coerced format
    '''
    data = _coerce_bson_recursive(data)
    return data

def _query(collection, key_names, args, error_handler):
    try:
        query = {key: _coerce_bson(val) for key, val in zip(key_names, args)}
        cursor = collection.find(query)
        docs = list(cursor)

        # print(docs)

        if len(docs) > 0:
            return _process_query(docs[0])
        else:
            return None
    except:
        error_handler()
        return None
    
def _push_data(collection, data, key_names, args, error_handler):
    '''
        Inputs:
            collection : collection object to push data into
            data : function output data to cache
            *args : inputs to function
    '''
    try:
        # make copy of data for processing and cacheing
        # do not alter original output
        data = deepcopy(data)

        entry = {key: val for key, val in zip(key_names, args)}
        entry['response'] = data
        entry['timestamp'] = datetime.datetime.now()

        # coerce data into BSON types mentioned in the schema
        data = _coerce_bson(entry)
        collection.insert_one(entry)
    except Exception as ex:
        print("error cacheing data", ex)
        error_handler()
