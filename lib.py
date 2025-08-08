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
from collections import OrderedDict
from typing import Tuple, Dict, List  # noqa: F401

# 3rd party
import pandas as pd
import psutil

# local
import constants
from constants import (
    DATA_FILEPATH,
    SEARCH_OPTIMAL_FILEPATH,
    DRIVE,
    VALUE,
    DURATION,
    ITERATIONS,
    MB,
    GB,
)


def abspath(*paths):
    return os.path.abspath(os.path.expanduser(os.path.join(*paths)))


def get_keys_from_dicts(*dicts):
    keys = []
    for dick in dicts:
        for key in dick.keys():
            if key not in keys:
                keys.append(key)
    return keys


def create_bytearray(count, value=VALUE):
    if value == VALUE:
        new = bytearray(random.randint(0, 255) for _ in range(count))
    else:
        new = bytearray(value for _ in range(count))
    return new


def create_bytearray_killobytes(count, value=VALUE, cheat=True):
    '''
    Description:
        create a bytearray by repeating only 1kb until the requested count
        the motivation is i've found generating 1MB takes about .15s,
            but generating 10MB takes 20s, so its not growing linearly.
    '''
    logging.debug('%s, value=%s', count, value)
    if cheat:
        killobyte = create_bytearray(1024, value=value)
        killobytes_array = bytearray()
        for _ in range(count):
            killobytes_array.extend(killobyte)
    else:
        killobytes_array = create_bytearray(1024 * count, value=value)
    logging.debug('created byte array of size %0.3f MB', len(killobytes_array) / MB)
    return killobytes_array


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


def disk_usage_monitor(event, drive=DRIVE):
    # type: (threading.Event, str) -> None
    prior_percent = None
    while not event.is_set():
        du = psutil.disk_usage(drive)
        if str(du.percent) != str(prior_percent):
            logging.debug('disk usage: %s%%', du.percent)
            prior_percent = str(du.percent)
        for _ in range(100):
            time.sleep(1 / 100)
            if event.is_set():
                break


def write_burnin(byte_array, data_filepath=DATA_FILEPATH, duration=DURATION, iterations=ITERATIONS):
    # type: (bytearray, str, float, int) -> Tuple[int, float, int]
    '''
    Description:
        given a bytearray, write it to the disk in write mode fashion until the duration or iterations has been exceeded
    Arguments:
        duration: float
            in seconds, how long should it go for? exec ends after duration exceeded or iteration exceeded
        iteration: int
            in ints, many times? exec ends after duration exceeded or iteration exceeded
    Returns:
        Tuple[int, float, int]
            bytes written, elapsed in seconds, iterations achieved
    '''
    logging.debug('data_filepath="%s", duration=%s, iterations=%s', data_filepath, duration, iterations)
    with open(data_filepath, 'wb'):
        pass
    original_size = os.path.getsize(data_filepath)
    with open(data_filepath, 'wb') as wb:
        start = time.time()
        iteration = 0
        # try:
        while time.time() - start < duration or iteration < iterations:
            wb.write(byte_array)
            iteration += 1
        # except KeyboardInterrupt:
        #     logging.warning('ctrl + c detected!')

        end = time.time()
    bytes_written = os.path.getsize(data_filepath) - original_size
    elapsed = end - start
    throughput = bytes_written / MB / elapsed
    logging.debug(
        'bytes_written=%s, elapsed=%s, iteration=%s, throughput=%0.3f MB/s', bytes_written, elapsed, iteration,
        throughput
    )
    return bytes_written, elapsed, iteration


def write_fulpak(byte_array, data_filepath=DATA_FILEPATH, duration=DURATION, iterations=-1, dumon=True):
    # type: (bytearray, str, int, int, bool) -> None
    '''
    Description:
        given a bytearray, write it to the disk in an appending fashion,
            and when you inevitably overshoot, value in 1mb increments
    Arguments:
        iterations: int
            if > 0, if iterations exceeded, break early
    Returns:
    '''
    if iterations == 0:
        raise ValueError('Doesnt make sense to run for 0 iterations!')
    logging.debug('data_filepath="%s"', data_filepath)

    drive, _ = os.path.splitdrive(abspath(data_filepath))

    byte_array_bytes = len(byte_array)
    if dumon:
        event = threading.Event()
        t = threading.Thread(target=disk_usage_monitor, args=(event, ), kwargs=dict(drive=drive), daemon=True)
        t.start()
    try:
        if iterations == 1:
            logging.debug('writing only once')
            with open(data_filepath, 'wb') as wb:
                wb.write(byte_array)
        else:
            # touch the file
            with open(data_filepath, 'w'):
                pass
            # write the bulk of the data
            with open(data_filepath, 'ab') as wb:
                start = time.time()
                iteration = 0
                while psutil.disk_usage(drive).free > byte_array_bytes:
                    if iterations != -1 and iteration % 10000 == 0:
                        logging.debug('i: %d', iteration)
                    wb.write(byte_array)
                    iteration += 1
                    if iterations != -1 and iteration == iterations:
                        logging.debug('ending fulpak write due to iterations %d exceeded', iterations)
                        break
                    if duration != -1 and time.time() - start > duration:
                        logging.debug(
                            'ending fulpak write due to elapsed %s > %s exceeded',
                            time.time() - start, duration
                        )
                        iteration += 1

                logging.debug('wrote %d times', iteration)
            if iterations == -1:
                # write the last chunk in 1mb increments until disk fills and raises OSError
                for i in range(byte_array_bytes // MB):
                    if psutil.disk_usage(drive).free > MB:
                        one_mb_array = byte_array[i * MB:(i + 1) * MB]
                        wb.write(one_mb_array)
                    else:
                        break
    except KeyboardInterrupt:
        logging.warning('ctrl + c detected, cancelling')
        raise
    except OSError:
        logging.info('done')
    finally:
        if dumon:
            event.set()
    du = psutil.disk_usage(drive)
    logging.debug('disk usage: %s%%', du.percent)


def create_byte_array_high_throughput(
    data_filepath=DATA_FILEPATH, search_optimal_filepath=SEARCH_OPTIMAL_FILEPATH, value=VALUE
):
    # type: (str, str, int) -> bytearray
    '''
    Description:
        create a bunch of byte_arrays of different sizes and pick the one with the highest write throughput
    Arguments:
        fill: int
            default -1
            from 0-255, do you want the bytes to be all the same, or -1 for random?
    Returns:
        bytearray
    '''
    logging.debug(
        'data_filepath="%s", search_optimal_filepath="%s", value=%s', data_filepath, search_optimal_filepath, value
    )
    rows = []
    sweetspot_bytearray = bytearray()
    sweetspot_killobytes = 0
    sweetspot_rate = 0.0
    killobytes_list = [1, 4, 32, 128]
    killobytes_list.extend([1024 * ele for ele in killobytes_list])
    killobytes_list.extend([ele * 2 for ele in killobytes_list] + [ele * 3 for ele in killobytes_list])
    for killobytes in sorted(killobytes_list):
        megabytes = killobytes / 1024
        byte_array = create_bytearray_killobytes(killobytes, value=value)
        bytes_written_bytes, elapsed, iteration = write_burnin(byte_array, data_filepath, duration=2.5, iterations=3)
        bytes_written_mb = bytes_written_bytes / MB
        rate = bytes_written_mb / elapsed
        if rate > sweetspot_rate:
            sweetspot_rate = rate
            sweetspot_killobytes = killobytes
            sweetspot_bytearray = byte_array
        logging.debug(
            '%s kb - %0.3f mb - %0.3f mb/s over %0.3f sec - iteration %s', killobytes, megabytes, rate, elapsed,
            iteration
        )
        row = {'kb': killobytes, 'mb': megabytes, 'rate': rate, 'elapsed': elapsed, 'iteration': iteration}
        rows.append(row)
    df = pd.DataFrame(rows)
    logging.debug('\n%s', df)
    df.to_csv(search_optimal_filepath, index=False)
    logging.info('%s kb - %0.3f mb/s - sweetspot', sweetspot_killobytes, sweetspot_rate)
    return sweetspot_bytearray


def write_bytearray_to_disk(byte_array, size=-1, data_filepath=DATA_FILEPATH, randomness=True):
    # type: (bytearray, int, str, bool) -> None
    '''
    Description:
        cleverly write a byte_array to the disk,
        especially if the provided array is meant to be repeatedly written until it hits the right size.
    '''
    if size == -1:
        size = len(byte_array)
    with open(data_filepath, 'wb') as wb:
        pass
    iterations = size // len(byte_array)
    with open(data_filepath, 'ab') as wb:
        prior = ''
        for i in range(1, iterations + 1):
            # basically "fake" randomness even further by starting at different points within the already created one.
            # if fill is provided, they're all constants anyway
            if randomness:
                midpoint = random.randint(0, len(byte_array) - 1)
                wb.write(byte_array[midpoint:])
                wb.write(byte_array[0:midpoint])
            else:
                wb.write(byte_array)

            getsize = os.path.getsize(data_filepath) / GB
            getsizestr = str(int(getsize))
            if getsizestr != prior and int(getsize) % 16 == 0:  # really slow it down
                logging.debug('%0.1f%% or %s GB written', i / iterations * 100, getsizestr)
                prior = getsizestr

        remainder = size - os.path.getsize(data_filepath)
        wb.write(byte_array[0:remainder])
        logging.info('%0.1f%% or %0.3f GB written', i / iterations * 100, os.path.getsize(data_filepath) / GB)


def read_bytearray_from_disk(byte_array, data_filepath=DATA_FILEPATH):
    # type: (bytearray, str) -> bool
    iterations = os.path.getsize(data_filepath) // len(byte_array)
    with open(data_filepath, 'rb') as rb:
        prior = ''
        i = 1
        read_array = rb.read(len(byte_array))
        while read_array:
            if len(read_array) == len(byte_array):
                assert read_array == byte_array, (
                    '\n'.join([f'on iteration {i}, full array read != write!'] + diff_bytes(read_array, byte_array))
                )
            else:
                sub_array = byte_array[:len(read_array)]
                assert read_array == sub_array, (
                    '\n'.join([f'on iteration {i}, sub full array read != write!'] + diff_bytes(read_array, sub_array))
                )

            read_array = rb.read(len(byte_array))
            i += 1
            getsize = len(byte_array) * i / GB
            getsizestr = str(int(getsize))
            if prior != getsizestr and int(getsize) % 16 == 0:  # really slow it down
                logging.info('%0.1f%% or %s GB read', i / (iterations) * 100, getsizestr)
                prior = getsizestr

    getsize = len(byte_array) * i / GB
    logging.info('%0.1f%% or %0.3f GB read', i / (iterations) * 100, getsize)
    return True


def generate_and_write_bytearray(
    size, value=VALUE, no_optimizations=False, data_filepath=DATA_FILEPATH, randomness=False
):
    # type: (int, int, bool, str, bool) -> None
    '''
    Description:
        basically just create a file of a certain size.
        TODO: be wary of too large sizes on no_optimizations
    '''
    logging.debug('%s, value=%s, no_optimizations=%s, data_filepath="%s"', size, value, no_optimizations, data_filepath)
    if no_optimizations:
        byte_array = create_bytearray(size, value=value)
    else:
        if size < MB:
            byte_array = create_bytearray(size, value=value)
        else:
            byte_array = create_bytearray(MB, value=value)  # 1mb is pretty performant no matter what
    write_bytearray_to_disk(byte_array, size=size, data_filepath=DATA_FILEPATH, randomness=randomness)


def crystaldiskinfo_parse(text):
    # type: (str) -> Dict[str, dict]
    content = text.splitlines()
    # crystal_disk name to disk number
    crystal_disks = {}  # type: dict
    crystal_data = {}  # type: dict
    crystal_disk = {}  # type: dict
    crystal_disk_list = []
    line = content.pop(0)
    try:
        while content:
            if line.startswith('-- '):
                if 'Disk List' in line:
                    while content:
                        line = content.pop(0)
                        if line == '-' * 76:
                            break
                        crystal_disk_list.append(line)

                    # logging.debug('crystal_disk_list: %s', json.dumps(crystal_disk_list, indent=2))
                    for line in crystal_disk_list:
                        line = line.rstrip()
                        if not line:
                            continue
                        key = line.split(' : ')[0]
                        disk_number = re.findall(r'[X\d]/[X\d]/[X\d]', line)[0][0]  # 5/4/0 means disk 5
                        crystal_disks[key] = disk_number
                    # logging.debug('crystal_disks: %s', json.dumps(crystal_disks, indent=2))
                elif 'S.M.A.R.T.' in line:
                    # -- S.M.A.R.T. --------------------------------------------------------------
                    # ID Cur Wor Thr RawValues(6) Attribute Name
                    # 05 100 100 __0 000000000000 Re-Allocated Sector Count
                    # 09 100 100 __0 0000000000D4 Power-On Hours Count
                    # 0C 100 100 __0 0000000000D2 Power Cycle Count
                    # ID RawValues(6) Attribute Name
                    # 01 000000000000 Critical Warning
                    line = content.pop(0)  # get rid of next line "ID RawValues(6) Attribute Name"
                    line = content.pop(0)  # 01 000000000000 Critical Warning
                    while line:
                        if not line:
                            break
                        try:
                            mo = re.match(
                                r'(?P<ID>[A-F0-9]{2,})(?P<whocares>[ _0-9]+)? (?P<RawValues>[A-F0-9]{12,}) (?P<AttributeName>.+)',  # noqa: E501
                                line
                            )
                            if not mo:
                                raise RuntimeError(f'regex doesnt match, line: {line}')
                            dick = mo.groupdict()
                        except Exception:
                            print(repr(line))
                            raise
                        RawValues, AttributeName = dick['RawValues'], dick['AttributeName']
                        crystal_disk[AttributeName] = int(RawValues, base=16)
                        line = content.pop(0)

            elif line in crystal_disks:
                crystal_disk['Disk Number'] = disk_number
                crystal_disk = {'datetime': str(datetime.datetime.now())}
                disk_number = crystal_disks[line]
                line = content.pop(0)  # remove first ('-' * 76)
                line = content.pop(0)  # get line right after, Model:
                while not line.startswith('-- '):
                    line = line.rstrip()
                    if not line:
                        break
                    try:
                        key, val = line.split(' :')
                    except Exception:
                        print(repr(line))
                        raise
                    crystal_disk[key.strip()] = val.strip()
                    line = content.pop(0)

                crystal_data[disk_number] = crystal_disk

            line = content.pop(0)
    except Exception:
        logging.error(line, exc_info=True)
        raise

    # logging.debug('disks: %s', json.dumps(crystal_disks, indent=2))
    # logging.debug('data: %s', json.dumps(crystal_data, indent=2))
    return crystal_data


def crystaldiskinfo():
    # type: () -> Dict[str, dict]
    # run crystaldiskinfo, get a text document (each time converting into telemetry data
    cmd = [constants.CRYSTALDISKINFO_EXE, '/CopyExit']
    # logging.debug(subprocess.list2cmdline(cmd))
    _ = subprocess.check_call(cmd, universal_newlines=True)

    # TODO: dynamic crystaldiskinfo txt
    if constants.CRYSTALDISKINFO_TXT == '':
        candidates = [
            abspath(os.path.dirname(constants.CRYSTALDISKINFO_EXE), 'DiskInfo.txt'),
            r'C:\ProgramData\chocolatey\lib\crystaldiskinfo.portable\tools\DiskInfo.txt',
            os.path.expanduser(r'~\Desktop\crystaldiskinfo.portable\tools'),
            r'C:\Program Files\CrystalDiskInfo\DiskInfo.txt',
        ]
        for candidate in candidates:
            if os.path.isfile(candidate):
                constants.CRYSTALDISKINFO_TXT = candidate
                break
        if not os.path.isfile(constants.CRYSTALDISKINFO_TXT):
            raise OSError('Could not find DiskInfo.txt! I looked everywhere!')
    with open(constants.CRYSTALDISKINFO_TXT, 'r', encoding='utf-8') as r:
        content = r.read()

    return crystaldiskinfo_parse(content)


FILE_SIZE_UNITS = OrderedDict({'p': 5, 't': 4, 'g': 3, 'm': 2, 'k': 1})
FILE_SIZE_UNITS['b'] = 0  # so that this is searched last
for k in list(FILE_SIZE_UNITS.keys()):
    if k != 'b':
        v = FILE_SIZE_UNITS[k]
        FILE_SIZE_UNITS['{}b'.format(k)] = v
BYTES_TO_SIZE_UNITS = ['b', 'k', 'm', 'g', 't', 'p']


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


def bytes_to_size(size, upper=False):
    """
    # https://github.com/x4nth055/pythoncode-tutorials/blob/master/general/process-monitor/process_monitor.py
    Returns size of bytes in a nice format
    """
    units = BYTES_TO_SIZE_UNITS if not upper else [e.upper() for e in BYTES_TO_SIZE_UNITS]
    for unit in units:
        if size < 1024:
            return '{:.2f}{}'.format(size, unit)
        size /= 1024
