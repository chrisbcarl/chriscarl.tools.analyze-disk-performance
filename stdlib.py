# stdlib
import os
import time
import argparse
import logging
import functools  # noqa: F401
import threading  # noqa: F401
from collections import OrderedDict
from typing import List, Optional, Callable, Any  # noqa: F401

# app
import constants

FILE_SIZE_UNITS = OrderedDict({'p': 5, 't': 4, 'g': 3, 'm': 2, 'k': 1})
FILE_SIZE_UNITS['b'] = 0  # so that this is searched last
for k in list(FILE_SIZE_UNITS.keys()):
    if k != 'b':
        v = FILE_SIZE_UNITS[k]
        FILE_SIZE_UNITS['{}b'.format(k)] = v
BYTES_TO_SIZE_UNITS = ['b', 'k', 'm', 'g', 't', 'p']


class NiceFormatter(
    argparse.ArgumentDefaultsHelpFormatter, argparse.RawTextHelpFormatter, argparse.RawDescriptionHelpFormatter
):
    pass


def abspath(*paths):
    return os.path.abspath(os.path.expanduser(os.path.join(*paths)))


def get_keys_from_dicts(*dicts):
    keys = []
    for dick in dicts:
        for key in dick.keys():
            if key not in keys:
                keys.append(key)
    return keys


def touch(filepath):
    with open(filepath, 'wb'):  # touch the file
        pass


def diff_bytes(byte_array_l, byte_array_r, max_diffs=10):
    # type: (bytearray|bytes, bytearray|bytes, int) -> List[str]
    diffs = []
    if len(byte_array_l) != len(byte_array_r):
        diffs = [f'length does not match: {len(byte_array_l)} != {len(byte_array_r)}']

    for i in range(max([len(byte_array_l), len(byte_array_r)])):
        bl, br = byte_array_l[i], byte_array_r[i]
        if bl != br:
            diffs.append(f'byte at index {i} unequal: {bl!r} != {br!r}')
        if len(diffs) > max_diffs:
            break

    return diffs


def size_unit_convert(size, into='b'):
    '''
    ex) size_unit_convert('512mb')
    ex) size_unit_convert('1024.123 gb')
    '''
    into = into.lower()
    if into[-1] == 's':
        into = into[0:-1]
    if into not in FILE_SIZE_UNITS:
        raise ValueError('cannot convert "{}" into: "{}", must be of unit: {}'.format(size, into, FILE_SIZE_UNITS))

    size = str(size).lower().replace(' ', '')
    for unit in FILE_SIZE_UNITS:
        if unit in size:
            numeric = size.split(unit)[0]
            floating_point = '.' in numeric
            numeric = float(numeric) if floating_point else int(numeric)

            exponent = FILE_SIZE_UNITS[unit]
            numeric_bytes = numeric * 1024**exponent
            into_magnitude = (1024**FILE_SIZE_UNITS[into])
            return numeric_bytes / into_magnitude if floating_point else numeric_bytes // into_magnitude

    return int(size, base=0)


def bytes_to_size(size, upper=True, space=True):
    """
    # https://github.com/x4nth055/pythoncode-tutorials/blob/master/general/process-monitor/process_monitor.py
    Returns size of bytes in a nice format
    """
    units = BYTES_TO_SIZE_UNITS if not upper else [e.upper() for e in BYTES_TO_SIZE_UNITS]
    for unit in units:
        if size < 1024:
            return '{:.3f}{}{}{}'.format(size, ' ' if space else '', unit, 'B' if upper else 'b')
        size /= 1024


def countdown(duration, stop_event):
    # type: (float|int, threading.Event) -> bool
    start = time.time()
    while time.time() - start < duration:
        if stop_event.is_set():
            break
        time.sleep(0.01)
    stop_event.set()
    return True


def while_true(func, stop_event):
    # type: (Callable, threading.Event) -> Any
    while not stop_event.is_set():
        res = func()

    stop_event.set()
    return res


RESULT_BEHAVIORS = ['singleton', 'accumulate', 'append', 'map']


def loop_or_elapsed(
    func,
    description,
    cleanup=None,
    result_behavior='singleton',
    iterations=constants.ITERATIONS,
    duration=constants.DURATION,
    stop_event=constants.STOP_EVENT,
):
    # type: (Callable, str, Optional[Callable], str, int, float|int, threading.Event) -> Any|list|dict
    '''Run func function until EITHER duration or iterations has exceeded'''
    if iterations == constants.ITERATIONS and duration == constants.DURATION:
        raise ValueError('either iterations or duration MUST be non default!')
    if result_behavior not in RESULT_BEHAVIORS:
        raise TypeError(f'result_behavior {result_behavior!r} not in {RESULT_BEHAVIORS}!')

    if result_behavior == 'singleton':
        res = None
    elif result_behavior == 'accumulate':
        res = 0
    elif result_behavior == 'append':
        res = []  # type: ignore
    elif result_behavior == 'map':
        res = {}  # type: ignore

    iterations_exceeded = False

    timer = threading.Thread(target=countdown, args=(duration, stop_event), daemon=True)
    timer.start()

    try:
        start = time.time()
        iteration = 0
        while not stop_event.is_set():
            elapsed = time.time() - start
            logging.info(
                '%r loop until either %0.3f > %0.3f sec OR %d / %d', description, elapsed, duration, iteration,
                iterations
            )
            result = func()
            if result_behavior == 'singleton':
                res = result
            elif result_behavior == 'accumulate':
                res += result
            elif result_behavior == 'append':
                res.append(result)  # type: ignore
            elif result_behavior == 'map':
                res.update(result)  # type: ignore

            iteration += 1
            if iterations != constants.ITERATIONS and iteration == iterations:
                iterations_exceeded = True
                stop_event.set()
                break

        timer.join()
        elapsed = time.time() - start
        if iterations_exceeded:
            logging.info('%r iterations exceeded %d', description, iterations)
        else:
            logging.info('%r duration exceeded %0.3f > %0.3f sec', description, elapsed, duration)

    except KeyboardInterrupt:
        logging.warning('cancelling...')
        logging.debug('ctrl + c', exc_info=True)
        stop_event.set()
    except Exception:
        logging.exception('what the hell happened here?')
        raise
    finally:
        if callable(cleanup):
            logging.debug('cleaning up...')
            cleanup()

    return res


def loop(
    func,
    description,
    cleanup=None,
    result_behavior='singleton',
    iterations=constants.ITERATIONS,
    duration=constants.DURATION,
    stop_event=constants.STOP_EVENT,
):
    # type: (Callable, str, Optional[Callable], str, int, float|int, threading.Event) -> Any|list|dict
    if stop_event.is_set():
        raise RuntimeError('stop_event is set at the onset! something is wrong with exec order!')
    if result_behavior not in RESULT_BEHAVIORS:
        raise TypeError(f'result_behavior {result_behavior!r} not in {RESULT_BEHAVIORS}!')
    if result_behavior == 'singleton':
        res = None
    elif result_behavior == 'accumulate':
        res = 0
    elif result_behavior == 'append':
        res = []  # type: ignore
    elif result_behavior == 'map':
        res = {}  # type: ignore

    try:
        iteration = 1
        if iterations != constants.ITERATIONS:
            logging.info('%r will run until iterations exceeded %d', description, iterations)
            for iteration in range(1, iterations + 1):
                logging.info('%r loop %d / %d', description, iteration, iterations)
                result = func()
                if result_behavior == 'singleton':
                    res = result
                elif result_behavior == 'accumulate':
                    res += result
                elif result_behavior == 'append':
                    res.append(result)  # type: ignore
                elif result_behavior == 'map':
                    res.update(result)  # type: ignore

        elif duration != constants.DURATION:
            logging.info('%r will run until duration exceeded %0.3f', description, duration)

            timer = threading.Thread(target=countdown, args=(duration, stop_event), daemon=True)
            timer.start()

            start = time.time()
            while not stop_event.is_set():
                elapsed = time.time() - start
                logging.info('%r duration %0.3f / %0.3f sec', description, elapsed, duration)
                result = func()
                if result_behavior == 'singleton':
                    res = result
                elif result_behavior == 'accumulate':
                    res += result
                elif result_behavior == 'append':
                    res.append(result)  # type: ignore
                elif result_behavior == 'map':
                    res.update(result)  # type: ignore

            timer.join()
            elapsed = time.time() - start
            logging.info('%r duration exceeded %0.3f > %0.3f sec', description, elapsed, duration)

        else:
            start = time.time()
            elapsed = time.time() - start
            while True:
                logging.info('%r loop inf, elapsed: %0.3f sec, iteration %d', description, elapsed, iteration)
                result = func()
                if result_behavior == 'singleton':
                    res = result
                elif result_behavior == 'accumulate':
                    res += result
                elif result_behavior == 'append':
                    res.append(result)  # type: ignore
                elif result_behavior == 'map':
                    res.update(result)  # type: ignore

                iteration += 1
                elapsed = time.time() - start

    except KeyboardInterrupt:
        logging.warning('cancelling...')
        logging.debug('ctrl + c', exc_info=True)
        stop_event.set()
    except Exception:
        logging.exception('what the hell happened here?')
        raise
    finally:
        if callable(cleanup):
            logging.debug('cleaning up...')
            cleanup()

        return res
