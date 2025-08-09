# stdlib imports
import os
import sys
import time
import functools

import logging

ROOT_DIRPATH = os.path.dirname(os.path.dirname(__file__))

sys.path.insert(0, ROOT_DIRPATH)

# app imports
import stdlib
import constants

logging.basicConfig(
    format='%(asctime)s - %(levelname)10s - %(funcName)48s - %(message)s', level=logging.DEBUG, stream=sys.stdout
)


def plz(x=1, stop_event=constants.STOP_EVENT):
    print('plz, sleeping', x)
    start = time.time()
    for i in range(x):
        if stop_event.is_set():
            break
        time.sleep(1)
    return x, time.time() - start


def no(x=1, stop_event=constants.STOP_EVENT):
    print('cleanup, sleeping', x)
    for i in range(x):
        if stop_event.is_set():
            break
        time.sleep(1)


res = stdlib.loop(
    functools.partial(plz, x=2),
    'please-loop-duration',
    cleanup=functools.partial(no, x=2),
    result_behavior='append',
    duration=5,
)
print(res)
constants.STOP_EVENT.clear()

res = stdlib.loop(
    functools.partial(plz, x=2),
    'please-loop-iteration',
    cleanup=functools.partial(no, x=2),
    result_behavior='append',
    iterations=2,
)
print(res)
constants.STOP_EVENT.clear()

res = stdlib.loop_or_elapsed(
    functools.partial(plz, x=2),
    'please-OR-iteration-only',
    cleanup=functools.partial(no, x=2),
    result_behavior='append',
    iterations=2,
)
print(res)
constants.STOP_EVENT.clear()

res = stdlib.loop_or_elapsed(
    functools.partial(plz, x=2),
    'please-OR-duration-only',
    cleanup=functools.partial(no, x=2),
    result_behavior='append',
    duration=5,
)
print(res)
constants.STOP_EVENT.clear()

res = stdlib.loop_or_elapsed(
    functools.partial(plz, x=2),
    'please-OR-iteration+duration-iteration-win',
    cleanup=functools.partial(no, x=2),
    result_behavior='append',
    iterations=2,
    duration=5,
)
print(res)
constants.STOP_EVENT.clear()

res = stdlib.loop_or_elapsed(
    functools.partial(plz, x=2),
    'please-OR-iteration+duration-duration-win',
    cleanup=functools.partial(no, x=2),
    result_behavior='append',
    iterations=3,
    duration=5,
)
print(res)
constants.STOP_EVENT.clear()
