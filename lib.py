# stdlib
from __future__ import print_function, division
import os
import re
import time
import random
import logging
import datetime
import threading
import subprocess
from typing import Tuple, Dict, List  # noqa: F401

# 3rd party
import pandas as pd
import psutil

# local
import constants

# def write_bytearray_to_disk(
#     byte_array,
#     size=-1,
#     data_filepath=DATA_FILEPATH,
#     randomness=True,
#     log_unit=constants.LOG_UNIT,
#     log_mod=constants.LOG_MOD,
# ):
#     # type: (bytearray, int, str, bool, str, int) -> None
#     '''
#     Description:
#         cleverly write a byte_array to the disk,
#         especially if the provided array is meant to be repeatedly written until it hits the right size.

#     Arguments:
#         log_unit: str
#             default GB, how often do you want to see log files
#         log_mod: int
#             default 8, frequency of i/o that gets logged by EVERY OTHER units
#     '''
#     unit = constants.LOG_UNITS[log_unit]
#     if size == -1:
#         size = len(byte_array)
#     with open(data_filepath, 'wb') as wb:
#         pass
#     iterations = size // len(byte_array)
#     with open(data_filepath, 'ab') as wb:
#         prior = ''
#         for i in range(1, iterations + 1):
#             # basically "fake" randomness even further by starting at different points within the already created one.
#             # if fill is provided, they're all constants anyway
#             if randomness:
#                 midpoint = random.randint(0, len(byte_array) - 1)
#                 wb.write(byte_array[midpoint:])
#                 wb.write(byte_array[0:midpoint])
#             else:
#                 wb.write(byte_array)

#             getsize = os.path.getsize(data_filepath) / unit
#             getsizestr = str(int(getsize))
#             if getsizestr != prior and int(getsize) % log_mod == 0:  # really slow it down
#                 logging.debug('%0.3f%% or %0.3f %s written', i / iterations * 100, getsizestr, log_unit)
#                 prior = getsizestr

#         remainder = size - os.path.getsize(data_filepath)
#         wb.write(byte_array[0:remainder])
#         getsize = os.path.getsize(data_filepath) / GB
#         logging.info('%0.3f%% or %0.3f %s written', i / iterations * 100, getsize, log_unit)

# def read_bytearray_from_disk(
#     byte_array,
#     data_filepath=DATA_FILEPATH,
#     log_unit=constants.LOG_UNIT,
#     log_mod=constants.LOG_MOD,
# ):
#     # type: (bytearray, str, str, int) -> int
#     unit = constants.LOG_UNITS[log_unit]
#     iterations = os.path.getsize(data_filepath) // len(byte_array)
#     with open(data_filepath, 'rb') as rb:
#         prior = ''
#         i = 0
#         read_array = rb.read(len(byte_array))
#         while read_array:
#             i += 1
#             if len(read_array) == len(byte_array):
#                 assert read_array == byte_array, (
#                     '\n'.join([f'on iteration {i}, full array read != write!'] + diff_bytes(read_array, byte_array))
#                 )
#             else:
#                 sub_array = byte_array[:len(read_array)]
#                 assert read_array == sub_array, (
#                     '\n'.join([f'on iteration {i}, sub full array read != write!'] + diff_bytes(read_array, sub_array))
#                 )

#             read_array = rb.read(len(byte_array))
#             getsize = len(byte_array) * i / unit
#             getsizestr = str(int(getsize))
#             if prior != getsizestr and int(getsize) % log_mod == 0:  # really slow it down
#                 logging.debug('%0.3f%% or %0.3f %s read', i / (iterations) * 100, getsize, log_unit)
#                 prior = getsizestr

#     getsize = len(byte_array) * i / unit
#     logging.debug('%0.3f%% or %0.3f %s read', i / (iterations) * 100, getsize, log_unit)
#     return os.path.getsize(data_filepath)

# def write(
#     mode=constants.WRITE_MODE,
#     byte_array=None,
#     data_filepath=constants.DATA_FILEPATH,
#     size=constants.SIZE,
#     value=constants.VALUE,
#     iterations=constants.ITERATIONS,
#     duration=constants.DURATION,
#     no_delete=constants.NO_DELETE,
#     log_every=constants.LOG_EVERY,
#     stop_event=constants.STOP_EVENT,
#     **kwargs
# ):
#     # type: (str, Optional[bytearray], str, int, int, int, float, bool, int, threading.Event, Any) -> Any  # noqa: E501
#     '''
#     Description:
#         Write a file to the disk, perhaps random, repeatedly, fill the drive, set size, etc.

#         >>> write('/tmp/file')  # write random file to fill the disk, loop indefinitely
#         >>> write('/tmp/file', duration=10)  # write random file to fill the disk, break if elapsed exceeded, else loop
#         >>> write('/tmp/file', value=69, size=1024)  # write 1024 bytes of 64 to the same file infinitely (burn-in)

#     Arguments:
#         byte_array: Optional[bytearray]
#             use this bytearray instead of generating it (useful when passing one to the other)
#         data_filepath: str
#             the destination of the actual file to be written since we're operating at the OS level
#         size: int
#             -1 to auto-determine by testing a few sizes, else, size in in bytes to repeat or burnin
#         value: int
#             -1 for random, else, repeat the same value for all bytes
#         iterations: int
#             -1 for loop infinitely, else exec ends iteration exceeded (or duration exceeded)
#         duration: float|int
#             -1 for loop infinitely, else exec ends after elapsed exceeded in seconds (or duration exceeded)
#         no_delete: bool
#             default False, opt out of self-cleanup
#         log_every: int
#             log a progress report every X bytes
#         stop_event: threading.Event
#             a way to short circuit exit if stop_event.is_set()

#     Returns:
#         Any
#     '''
#     drive_letter, _ = os.path.splitdrive(data_filepath)
#     if size == constants.SIZE:
#         logging.info('creating bytearray...')
#         byte_array = create_efficient(data_filepath=data_filepath, value=value)
#     create = False
#     if isinstance(byte_array, bytearray):
#         if len(byte_array) == 0:
#             create = True
#         logging.info('reusing bytearray of size %s...', bytes_to_size(len(byte_array)))
#     else:
#         create = True
#     if create:
#         logging.info('creating bytearray...')
#         if size != constants.SIZE:
#             byte_array = create_bytearray(size, value=value)
#         else:
#             byte_array = create_efficient(data_filepath=data_filepath, value=value)

#     filesize = bytes_to_size(len(byte_array))  # type: ignore
#     summary = f'write {mode}, drive {drive_letter}, {filesize} file'
#     logging.warning(summary)

#     def writing():
#         touch(data_filepath)
#         if mode == 'burnin':
#             write_burnin(byte_array=byte_array, data_filepath=data_filepath)
#         elif mode == 'fulpak':
#             write_fulpak(byte_array, data_filepath=data_filepath, log_every=log_every, stop_event=stop_event)
#         else:
#             raise NotImplementedError(f'{mode!r} mode not implemented!')
#         if not no_delete:
#             touch(data_filepath)

#     res = loop(
#         writing,
#         summary,
#         cleanup=functools.partial(touch, data_filepath),
#         result_behavior='accumulate',
#         duration=10,
#     )
#     logging.info(res)
#     accumulators = [0] * len(res[0])
#     for tpl in res:
#         for t, ele in enumerate(tpl):
#             accumulators[t] += ele
#     logging.info(accumulators)

#     return accumulators
