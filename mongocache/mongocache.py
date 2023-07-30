'''
    TODO: disable cacheing on error; create disabler for that - DONE
    TODO: implement strict mode
    TODO: implement null handling - ALMOST DONE - EDGE CASE TESTING TBD
    TODO: error logging and warning logging
    TODO: make thread safe
    TODO: implement stale data definition: shelf_life - DONE
    TODO: implement all functionalities from cachier
    TODO: allow both remote and local cacheing
    TODO: allow user to define serializer - DONE
    TODO: enforce datatype (args, kwargs) and return types to be non-null
    TODO: ignore self argument - DONE
    TODO: support for unhashable types - DONE
    TODO: move data push and update to another thread
    TODO: max records - DONE
    TODO: LRU implementation -DONE
    TODO: max entry size allowed
    TODO: fallback
    TODO: optimize cache keys
'''

import os
import pymongo
import datetime
import json

from copy import deepcopy

from bson.decimal128 import Decimal128
from bson.timestamp import Timestamp

from mongocache.utils import _coerce_decimal128, _coerce_float, _coerce_timestamp, _coerce_datetime, _func_is_method, \
    _default_serialize, _default_deserialize, _default_hash_func

from mongocache.constants import ATOMIC_BSON_TYPES, BUILTIN_ITERABLES, MAX_RECORDS, CACHE_FLAGS, LOCAL_DIR_NAME

import logging
log_formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
logger = logging.getLogger()

console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
console_handler.setLevel(logging.DEBUG)
logger.addHandler(console_handler)



def mongocache(db_name, collection_name, port=27017, schema=None, shelf_life=datetime.timedelta.max, 
                hash_func=None, serialize=None, deserialize=None,
                max_records=MAX_RECORDS, lru_cache=True):
    '''
        decorator factory callable that creates and returns the wrapper decorator
    '''

    # create local directory
    localdir = os.path.join(os.getenv('APPDATA'), LOCAL_DIR_NAME, db_name, collection_name)
    if not os.path.exists(localdir):
        os.makedirs(localdir) 

    # create file logger
    file_handler = logging.FileHandler(os.path.join(localdir, ".log"))
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.WARNING)
    logger.addHandler(file_handler)

    sentinel = object()

    # hit-miss statistics
    hits, misses, errors = 0, 0, 0

    cache = MongoCache(port, db_name, collection_name, shelf_life, schema, \
                        hash_func=hash_func, serialize=serialize, deserialize=deserialize, \
                        max_records=max_records, lru_cache=lru_cache)
    def mongo_decorator(func):
        def func_wrapper(*args, **kwargs):
            nonlocal schema
            nonlocal hits, misses, errors, sentinel
            nonlocal func
            nonlocal cache

            # runtime function call arguments
            ignore_cache = kwargs.pop('ignore_index', False)
            overwrite_cache = kwargs.pop('overwrite_cache', False)

            # TODO: make collection object creation thread safe
            # after they are created in a thread safe manner thread safety for all operations
            # on the collection object is implemented by mongoDB
            if cache.enabled and not ignore_cache:
                data = sentinel

                # get list of arguments including kwargs sorted according to key
                all_args = args + tuple(dict(sorted(kwargs.items())).values())

                # remove self argument
                if _func_is_method(func):
                    all_args = all_args[1:]

                if cache.collection is None:
                    '''on first function call create a collection if not already created'''

                    # create key names on first call
                    cache.key_names = tuple([f"key_{i}" for i in range(len(all_args))])

                    # get function output to cache
                    data = func(*args, **kwargs)

                    # create schema from data and key_names if not given
                    if schema is None:
                        cache._create_schema(data, all_args)

                    # create collection
                    cache._create_collection()
                    logger.info("created new collection")

                    # push data
                    cache._push_doc(data, all_args)
                else:
                    # get key names from collection
                    cache.key_names = cache._get_keynames()

                    if overwrite_cache:
                        data = func(*args, **kwargs)
                        cache._update_doc(data, all_args)
                    else:
                        # query cache
                        document = cache._query(all_args)                

                        # update hit miss statistics
                        if document is not None:
                            data = document.get('response', sentinel)
                            if data is not sentinel:
                                logger.debug("cache hit")
                                hits += 1
                            else:
                                errors += 1
                        else:
                            ''' cache miss '''
                            data = sentinel
                            logger.debug("cache miss")
                            misses += 1


                        if data is sentinel:
                            '''cache miss - call function and cache output'''
                            data = func(*args, **kwargs)
                            cache._push_doc(data, all_args)

                return data
            else:
                '''cache is disabled'''
                return func(*args, **kwargs)
        
        return func_wrapper
    return mongo_decorator


class MongoCache():
    _INDEX_NAME = 'mongocache_index'

    def __init__(self, port, db_name, collection_name, shelf_life, schema=None, \
                    hash_func=None, serialize=None, deserialize=None, \
                    max_records=MAX_RECORDS, lru_cache=True):
        self._PORT = port
        self._DB_NAME = db_name
        self._COLLECTION_NAME = collection_name
        self._SCHEMA = schema
        self._SHELF_LIFE = shelf_life

        self._enabled = True
        self._key_names = []
        self.mongo_op_object = dict()

        self._hash_func = hash_func if hash_func is not None else _default_hash_func
        self._serialize = serialize if serialize is not None else _default_serialize
        self._deserialize = deserialize if deserialize is not None else _default_deserialize

        self.max_records = max_records
        self.lru_cache = lru_cache

        self.collection = None

        self._pre_run_tests()

    @property
    def enabled(self):
        return self._enabled

    def disable_cache(self):
        self._enabled = False

    def _pre_run_tests(self):
        try:
            # create connection
            client = pymongo.MongoClient("localhost", self._PORT)
        except:
            # connection failed
            # log error and disable cacheing
            logger.critical("connection failed...cacheing disabled ")
            self.disable_cache()
        else:
            # create database if not exists
            try:
                self.db = client[self._DB_NAME]
            except:
                logger.critical("error in creating/accessing database")
                self.disable_cache()
            else:
                # create collection if not exists
                try:
                    self.collection = self.db[self._COLLECTION_NAME]
                except:
                    logger.critical("error in creating/accessing collection")
                    self.disable_cache()
                else:
                    # count documents
                    self.doc_count = self.collection.count_documents({})

                    if self.doc_count >= self.max_records and not self.lru_cache:
                        self.disable_cache()

            client.close()


    @property
    def key_names(self):
        return self._key_names
    
    @key_names.setter
    def key_names(self, value):
        self._key_names = value

    def _get_keynames(self):
        index_info = self.collection.index_information().get(self._INDEX_NAME, None)

        if index_info is None:
            logger.critical("index not found...cacheing disabled")
            self.disable_cache()
            key_names = None
        else:
            key_names = [key[0] for key in index_info['key']]
        
        return key_names

    def _create_schema_recursive(self, data):
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
            item_bson_types = [self._create_schema_recursive(item) for item in data]

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
            properties = {key: self._create_schema_recursive(val) for key, val in data.items()}

            return {
                'bsonType'  : 'object',
                'properties': properties
            }
        else:
            raise RuntimeError(f"could not convert data of type {type(data)} into known BSON types")
        
    def _add_unique_clause(self, sub_schema):
        if sub_schema['bsonType'] == 'object':
            for key in sub_schema['properties']:
                if 'key'  in key:
                    sub_schema['properties'][key] = self._add_unique_clause(sub_schema['properties'][key])
        elif sub_schema['bsonType'] == 'array':
            sub_schema['items'] = self._add_unique_clause(sub_schema['items'])
        else:
            sub_schema['unique'] = True
        return sub_schema
    
    def _add_nullable(self, sub_schema):
        if sub_schema['bsonType'] == 'object':
            for key in sub_schema['properties']:
                sub_schema['properties'][key] = self._add_nullable(sub_schema['properties'][key])
        elif sub_schema['bsonType'] == 'array':
            sub_schema['items'] = self._add_nullable(sub_schema['items'])

        sub_schema['bsonType'] = [sub_schema['bsonType'], 'null']

        return sub_schema

    def _create_schema(self, data, args):
        '''
            Input:
                data: sample data
                key_names: names of keys corresponging to inputs args and kwargs
                args: list of arguments to the cached function
        '''

        # construct entry
        entry = {}

        for key_name, arg in zip(self.key_names, args):
            entry[key_name] = self._hash_func(arg)

        entry['response'] = data

        # iterate through the entry recursively to get the schema
        # throw error when an entry does not match a BSON type
        try:
            schema = self._create_schema_recursive(entry)
        except Exception as ex:
            logger.critical("error generating schema", ex)
            self.disable_cache()
            return

        # schema for key_names must have an unique clause
        # for key in schema['properties']:
        #     if 'key'  in key:
        #         schema['properties'][key] = _add_unique_clause(schema['properties'][key])

        # add nullable condition to response
        schema['properties']['response'] = self._add_nullable(schema['properties']['response'])

        # add timestamp to schema
        schema['properties'].update({
            "timestamp": {
                "bsonType": "timestamp"
            }
        })

        with open("test.json", 'w') as f:
            json.dump(schema, f, indent=4)

        self._SCHEMA = schema

    def _set_flags(self):
        if CACHE_FLAGS['allowDiskUseByDefault']:
            try:
                self.client.admin.command(({'getParameter': 1, 'allowDiskUseByDefault': 1}))
            except:
                logger.warning("error setting allowDiskUseByDefault flag on monodb server")

    def _create_collection(self):
        '''
            Input:
                - port: port id
                - db_name: name of database
                - collection_name: name of collection
                - key_names: key names for creating index
                - schema: schema for collection
        '''
        try:
            # create database
            self.client = pymongo.MongoClient("localhost", self._PORT)
            self.db = self.client[self._DB_NAME]

            logger.info("created database")

            # create collection
            if self._COLLECTION_NAME not in self.db.list_collection_names():
                self.db.create_collection(
                    self._COLLECTION_NAME,
                    validator = {
                        '$jsonSchema': self._SCHEMA
                    }
                )
            self.collection = self.db[self._COLLECTION_NAME]
            logger.info("created collection")

            # create compound index using cache keys and timestamp (for LRU implementation)
            # use E-S-R rule
            # cache keys use exact match
            # timestamp is only used for sorting
            indices = [(key, pymongo.ASCENDING) for key in self.key_names] + [("timestamp", pymongo.DESCENDING)]
            self.collection.create_index(indices, unique=True, name=self._INDEX_NAME)

            logger.info("created index")
        except Exception as ex:
            # log error
            logger.critical('error creating collection', exc_info=True)
            self.disable_cache()
        else:
            self._set_flags()

    def _query(self,args):
        try:
            # hash cache keys
            query = {key: self._hash_func(val) for key, val in zip(self.key_names, args)}

            # serialize hashed keys
            query = self._serialize(query)

            # query the db
            cursor = self.collection.find(query, {'_id': False})
            docs = list(cursor)

            # print(docs)

            if len(docs) > 0:
                # deserialize query result
                document = self._deserialize(docs[0])

                print(document['timestamp'])

                # check if data is stale
                if datetime.datetime.now() - document['timestamp'] <= self._SHELF_LIFE:
                    del document['timestamp']
                    return document
                else:
                    return None
            else:
                return None
        except Exception as ex:
            logger.error("disabling cache", exc_info=True)
            self.disable_cache()
            return None

    def _get_timestamp(self):
        # add timestamp and inc
        timestamp = datetime.datetime.now()

        last_second = self.mongo_op_object.get("second", -1)
        op_count = self.mongo_op_object.get('op_count', 0)

        if timestamp.second == last_second:
            op_count += 1
        else:
            # reset
            self.mongo_op_object['second'] = timestamp.second
            op_count = 1

        self.mongo_op_object['op_count'] = op_count

        return timestamp, op_count

    def _update_doc(self, data, args):
        try:
            # make copy of data for processing and cacheing
            # do not alter original output
            data = deepcopy(data)

            # hash then serialize cache keys
            filter = self._serialize({key: self._hash_func(val) for key, val in zip(self.key_names, args)})

            # serialize data to be upserted
            update = {"$set": {
                'response': self._serialize(data), 
                'timestamp': _coerce_timestamp(*self._get_timestamp())
            }}

            # update and upsert
            self.collection.update_one(filter, update, upsert=True)
        except Exception as ex:
            logger.error("error overwriting data", exc_info=True)
            self.disable_cache()

    def _delete_lru_doc(self):
        # TODO: sort operation uses primary memory by default, but if server requires more than 100 MB for a pipeline execution stage
        # it needs to use disk for sorting - to allow for handling of large datasets, set allowDiskUse option to true
        self.collection.find_one_and_delete({"timestamp": {"$exists": True}}, sort=[("timestamp", pymongo.ASCENDING)])

    def _push_doc(self, data, args):
        '''
            Inputs:
                collection : collection object to push data into
                data : function output data to cache
                args : inputs to function
        '''
        logger.debug('cacheing data')
        try:
            self.doc_count = self.collection.count_documents({})

            if self.doc_count >= self.max_records:
                if self.lru_cache:
                    # delete least recently used document
                    self._delete_lru_doc()

            # make copy of data for processing and cacheing
            # do not alter original output
            data = deepcopy(data)

            # hash cache keys
            entry = {key: self._hash_func(val) for key, val in zip(self.key_names, args)}

            # pack entry
            entry['response'] = data

            # add timestamp
            entry['timestamp'] = _coerce_timestamp(*self._get_timestamp())

            # serialize entry
            data = self._serialize(entry)

            # insert entry
            self.collection.insert_one(entry)
        except Exception as ex:
            logger.error("error cacheing data", ex)
            self.disable_cache()
