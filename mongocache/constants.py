import datetime
import re

from bson.decimal128 import Decimal128
from bson.timestamp import Timestamp

from mongocache.utils import _coerce_decimal128, _coerce_float, _coerce_timestamp, _coerce_datetime

INDEX_NAME = 'mongocache_index'

MAX_RECORDS = float('inf')

LOCAL_DIR_NAME = 'mongocache'

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
    float   : _coerce_decimal128
}

ATOMIC_PYTHON_CONVERTERS = {
    Decimal128  : _coerce_float,
    Timestamp   : _coerce_datetime
}

BUILTIN_ITERABLES = frozenset([list, tuple, set, frozenset])

CACHE_FLAGS = {
    'allowDiskUseByDefault': True,
    'strict': True
}
