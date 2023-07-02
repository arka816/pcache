from decimal import Decimal, localcontext
import threading
import time

from bson.decimal128 import Decimal128, _decimal_to_128, create_decimal128_context
from bson.timestamp import Timestamp

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

def _coerce_timestamp(date):
    return Timestamp(date, 0)

def _coerce_datetime(timestamp):
    return timestamp.as_datetime()
