from decimal import Decimal, localcontext
import threading
import time
import inspect

import hashlib
import pickle

from bson.decimal128 import Decimal128, _decimal_to_128, create_decimal128_context
from bson.timestamp import Timestamp

from mongocache.constants import ATOMIC_BSON_TYPES, ATOMIC_BSON_CONVERTERS, ATOMIC_PYTHON_CONVERTERS, BUILTIN_ITERABLES


def _func_is_method(func):
    func_params = list(inspect.signature(func).parameters)
    return func_params and func_params[0] == 'self'

class set_interval:
    def __init__(self, action, interval):
        self.interval=interval
        self.action=action
        self.stop_event=threading.Event()

        thread=threading.Thread(target=self._set_interval)
        thread.start()

    def _set_interval(self):
        next_time = time.time() + self.interval

        while not self.stop_event.wait(next_time - time.time()):
            next_time += self.interval
            self.action()

    def cancel(self):
        self.stop_event.set()


def _coerce_decimal128(num):
    '''
        convert python floating point numbers to decimal 128 format

        - convert float to string with 34 decimal places precision since BID decimal 128 has 34 significand digits
        - with decimal128 context convert string to decimal128 
    '''
    num_string = format(num, ".34f")

    decimal128_ctx = create_decimal128_context()
    with localcontext(decimal128_ctx) as ctx:
        num = Decimal128(ctx.create_decimal(num_string))
    
    return num

def _coerce_float(num):
    return float(str(num))

def _check_offset_awareness(timestamp):
    return timestamp.tzinfo is not None and timestamp.tzinfo.utcoffset(timestamp) is not None

def _coerce_timestamp(date, inc):
    return Timestamp(date, inc)

def _coerce_datetime(timestamp):
    timestamp = timestamp.as_datetime().replace(tzinfo=None)
    return timestamp

def _default_serialize(data):
    '''
        recursively iterate through the data structure
        and convert pythonic builtin data types to BSON format
    '''
    if type(data) in ATOMIC_BSON_CONVERTERS:
        return ATOMIC_BSON_CONVERTERS[type(data)](data)
    elif type(data) in BUILTIN_ITERABLES:
        return [_default_serialize(item) for item in data]
    elif type(data) == dict:
        return {key: _default_serialize(val) for key, val in data.items()}
    return data

def _default_deserialize(data):
    '''
        recursively iterate through the data structure
        and convert BSON data from queried document to pythonic builtins
    '''
    if type(data) in ATOMIC_PYTHON_CONVERTERS:
        return ATOMIC_PYTHON_CONVERTERS[type(data)](data)
    elif type(data) in BUILTIN_ITERABLES:
        return [_default_deserialize(item) for item in data]
    elif type(data) == dict:
        return {key: _default_deserialize(val) for key, val in data.items()}
    return data

def _default_hash_func(data):
    '''
        recursively iterate through the data structure
        and hash non-serializable data
    '''
    # TODO: hash only non serializable types
    # for serializable types return as is

    if type(data) in ATOMIC_BSON_TYPES:
        return data
    elif type(data) in BUILTIN_ITERABLES:
        return [_default_hash_func(item) for item in data]
    elif type(data) == dict:
        return {key: _default_hash_func(val) for key, val in data.items()}
    else:
        # non-serializable types - HASH
        hash = hashlib.sha256()
        hash.update(pickle.dumps(data))
        return hash.hexdigest()
