# stdlib
import os
import time
import math
import random
import logging
import threading  # noqa: F401
from typing import Any, Tuple, Optional  # noqa: F401

# third party
import psutil
import pandas as pd

# app
import constants as con
from stdlib import touch, bytes_to_size, diff_bytes

SCRIPT_DIRPATH = os.path.abspath(os.path.dirname(__file__))


def create_bytearray(
    size=con.MB,
    value=con.VALUE,
    no_cheat=con.NO_CHEAT,
    stop_event=con.STOP_EVENT,
    **kwargs,
):
    # type: (int, int, bool, threading.Event, Any) -> bytearray
    '''
    Description:
        create a bytearray
        cheating is useful by repeating 1MB rather than allocating the entire 1GB

    Arguments:
        size: int
            what it says on the tin
        value: int
            default -1, else repeat this value "count" times
        no_cheat: bool
            default False, if True, dont apply this one neat trick
            if size > 1MB, simply repeat 1MB until size is filled up
        log_every: int
            default 1GB, log a progress report every X bytes
        stop_event: threading.Event
            a way to short circuit exit if stop_event.is_set()
        **kwargs: varkwarguments

    Returns:
        bytearray
    '''
    logging.debug('%s, value=%s, no_cheat=%s', bytes_to_size(size), value, no_cheat)
    if size > con.MB:
        if not no_cheat:
            mb = create_bytearray(con.MB, value=value, no_cheat=True)
            new = bytearray()

            try:
                idxes = list(range(size // con.MB))
                i_divs = int(math.log10(len(idxes))) - 1
                if i_divs < 0:
                    i_divs = 0
                i_divs = 10**i_divs * 5
                for i, _ in enumerate(idxes):
                    if i % i_divs == 0:
                        if stop_event.is_set():
                            raise KeyboardInterrupt('stop_event triggered by someone else')
                        logging.info('%0.3f%%', (i + 1) / len(idxes) * 100)
                    new.extend(mb)
                remainder = size % con.MB
                if remainder > 0:
                    new.extend(mb[:remainder])
            except KeyboardInterrupt:
                logging.warning('ctrl + c detected, deallocating')
                raise
    else:
        if value == con.VALUE:
            new = bytearray(random.randint(0, 255) for _ in range(size))
        else:
            new = bytearray(value for _ in range(size))

    return new


def create_efficient(
    data_filepath=con.DATA_FILEPATH,
    value=con.VALUE,
    stop_event=con.STOP_EVENT,
    **kwargs,
):
    # type: (str, int, threading.Event, Any) -> bytearray
    '''
    Description:
        create a bunch of byte_arrays of different sizes and pick the one with the highest write throughput
    Arguments:

        data_filepath: str
            the destination of the actual file to be written since we're operating at the OS level
        value: int
            -1 for random, else, [0,255] repeat the same value for all bytes
        stop_event: threading.Event
            a way to short circuit exit if stop_event.is_set()

    Returns:
        bytearray
    '''
    logging.debug('data_filepath="%s", value=%s', data_filepath, value)
    rows = []
    sweetspot_bytearray = bytearray()
    sweetspot_killobytes = 0
    sweetspot_rate = 0.0
    killobytes_list = [1, 4, 16]  # , 32, 128
    killobytes_list.extend([1024 * ele for ele in killobytes_list])
    killobytes_list.extend([ele * 2 for ele in killobytes_list] + [ele * 3 for ele in killobytes_list])
    for k, killobytes in enumerate(sorted(killobytes_list)):
        if stop_event.is_set():
            raise KeyboardInterrupt('stop_event triggered by someone else')
        bytes_size = killobytes * con.KB
        logging.debug('%s / %s - %s', k + 1, len(killobytes_list), bytes_to_size(bytes_size))
        megabytes = killobytes / 1024
        byte_array = create_bytearray(killobytes * con.KB, value=value)
        bytes_written_bytes, elapsed, _ = write_fast_append_remove(
            byte_array, data_filepath, duration=6.9, iterations=5
        )
        bytes_written_mb = bytes_written_bytes / con.MB
        rate = bytes_written_mb / elapsed
        if rate > sweetspot_rate:
            sweetspot_rate = rate
            sweetspot_killobytes = killobytes
            sweetspot_bytearray = byte_array
        logging.info(
            '%s / %s - attempting %s - %0.3f mb/s over %0.3f sec', k + 1, len(killobytes_list),
            bytes_to_size(bytes_size), rate, elapsed
        )
        row = {'kb': killobytes, 'mb': megabytes, 'rate': rate, 'elapsed': elapsed}
        rows.append(row)

    df = pd.DataFrame(rows)
    logging.debug('efficient throughputs\n%s', df.to_string(index=False))
    # df.to_csv(, index=False)
    logging.info('%s kb - %0.3f mb/s - sweetspot', sweetspot_killobytes, sweetspot_rate)
    return sweetspot_bytearray


def create(
    data_filepath=con.DATA_FILEPATH,
    size=con.SIZE,
    value=con.VALUE,
    no_cheat=con.NO_CHEAT,
    stop_event=con.STOP_EVENT,
    **kwargs,
):
    # type: (str, int, int, bool, threading.Event, Any) -> bytearray
    '''
    Description:
        Generate a bytearray based on inputs

    Arguments:
        data_filepath: str
            the destination of the actual file to be written since we're operating at the OS level
        size: int
            -1 to auto-determine by testing a few sizes, else, size in in bytes to repeat or burnin
        value: int
            -1 for random, else, [0,255] repeat the same value for all bytes
        no_cheat: bool
            default False, if True, dont apply this one neat trick
            if size > 1MB, simply repeat 1MB until size is filled up
        stop_event: threading.Event
            a way to short circuit exit if stop_event.is_set()
        **kwargs: varkwarguments

    Returns:
        bytearray
    '''
    logging.debug('data_filepath="%s", size=%s, value=%s, no_cheat=%s', data_filepath, size, value, no_cheat)
    if size == con.SIZE:
        logging.info('creating efficient bytearray...')
        byte_array = create_efficient(
            data_filepath=data_filepath, value=value, no_cheat=no_cheat, stop_event=stop_event
        )
    else:
        logging.info('creating explicit bytearray of size %s...', bytes_to_size(size))
        byte_array = create_bytearray(size, value=value, no_cheat=no_cheat, stop_event=stop_event)

    logging.debug('writing bytearray to "%s"', data_filepath)
    with open(data_filepath, 'wb') as wb:
        wb.write(byte_array)

    logging.info(
        'created bytearray at "%s" of size %s, first 16 bytes: %s', data_filepath, bytes_to_size(len(byte_array)),
        byte_array[:16]
    )
    return byte_array


def get_byte_array(
    byte_array=None,
    data_filepath=con.DATA_FILEPATH,
    size=con.SIZE,
    value=con.VALUE,
    no_cheat=con.NO_CHEAT,
    stop_event=con.STOP_EVENT,
):
    # type: (Optional[bytearray], str, int, int, bool, threading.Event) -> bytearray
    if isinstance(byte_array, bytearray) and len(byte_array) > 0:
        logging.info(
            'reusing byte_array of %s from kwarg, first 16 bytes: %s', bytes_to_size(len(byte_array)), byte_array[:16]
        )
        return byte_array

    if size == con.SIZE:
        byte_array = create(
            data_filepath=data_filepath, size=size, value=value, no_cheat=no_cheat, stop_event=stop_event
        )
        return byte_array

    if os.path.isfile(data_filepath):
        logging.debug('loading file from "%s" of %s', data_filepath, bytes_to_size(os.path.getsize(data_filepath)))
        with open(data_filepath, 'rb') as rb:
            byte_array = bytearray(rb.read())
        size_equal = (size == len(byte_array) if size != con.SIZE else True)
        values_equal = (all(bte == value for bte in byte_array[:16]) if value != con.VALUE else True)
        if size_equal and values_equal:
            logging.info(
                'loaded file from "%s" of %s, first 16 bytes: %s', data_filepath,
                bytes_to_size(os.path.getsize(data_filepath)), byte_array[:16]
            )
            return byte_array
        else:
            logging.info('stale file from "%s", need to regenerate', data_filepath)
    byte_array = create(data_filepath=data_filepath, size=size, value=value, no_cheat=no_cheat, stop_event=stop_event)
    return byte_array


def write_fast_append_remove(
    byte_array,
    data_filepath,
    duration=6.9,
    iterations=5,
):
    # type: (bytearray, str, int|float, int) -> Tuple[int, float, bytearray]
    touch(data_filepath)
    iteration = 0
    start = time.time()
    bytes_written = 0
    with open(data_filepath, 'ab') as ab:
        while True:
            iteration += 1

            bytes_written += ab.write(byte_array)

            elapsed = time.time() - start
            if iteration >= iterations and elapsed > duration:
                break
    os.remove(data_filepath)
    return bytes_written, elapsed, byte_array


def write_burnin(
    byte_array=None,
    data_filepath=con.DATA_FILEPATH,
    size=con.SIZE,
    value=con.VALUE,
    chunk_size=con.CHUNK_SIZE,
    log_every=con.LOG_EVERY,
    no_cheat=con.NO_CHEAT,
    no_delete=con.NO_DELETE,
    stop_event=con.STOP_EVENT,
    **kwargs
):
    # type: (Optional[bytearray], str, int, int, int, int, bool, bool, threading.Event, Any) -> Tuple[int, float, bytearray]  # noqa: E501
    '''
    Description:
        Optional bytearray, write it to the disk in write mode fashion until the duration or iterations has exceeded

    Arguments:
        byte_array: Optional[bytearray]
            use this bytearray and write to data_filepath
        data_filepath: str
            the destination of the actual file to be written since we're operating at the OS level
        size: int
            -1 to auto-determine by testing a few sizes, else, size in in bytes to repeat or burnin
        value: int
            -1 for random, else, [0,255] repeat the same value for all bytes
        chunk_size: int
            default 1MB, MUST evenly divide byte_array length
                instead of sequentially asserting, randomly dart around the file
                say the byte_array length 32, data_filepath length 64, chunk_size 4
                we will generate 64 / 4 = 16 "windows" to jump around and compare
        log_every: int
            default 1GB, log a progress report every X bytes
        no_cheat: bool
            default False, if True, dont apply this one neat trick
            if size > 1MB, simply repeat 1MB until size is filled up
        no_delete: bool
            default False, opt out of self-cleanup
        stop_event: threading.Event
            a way to short circuit exit if stop_event.is_set()
        **kwargs: varkwarguments

    Returns:
        Tuple[int, float, bytearray]
            bytes operated, elapsed in seconds, byte_array
    '''
    byte_array = get_byte_array(
        byte_array=byte_array,
        data_filepath=data_filepath,
        size=size,
        value=value,
        no_cheat=no_cheat,
        stop_event=stop_event,
    )
    if not isinstance(byte_array, bytearray):
        raise TypeError(f'byte_array must be of type bytearray, provided {type(byte_array)}!')
    logging.debug('byte_array=%s, data_filepath="%s"', bytes_to_size(len(byte_array)), data_filepath)
    logging.info('write_burnin with byte_array of %s', bytes_to_size(len(byte_array)))

    drive_letter, _ = os.path.splitdrive(data_filepath)
    bytes_written = 0
    prior_bytes = 0
    start = time.time()
    with open(data_filepath, 'wb') as wb:
        for i in range(0, len(byte_array), chunk_size):
            if stop_event.is_set():
                break
            bytes_written += wb.write(byte_array[i:i + chunk_size])
            if bytes_written > prior_bytes + log_every:
                end = time.time()
                elapsed = end - start
                if elapsed > 0:
                    throughput = bytes_written / elapsed
                du = psutil.disk_usage(drive_letter)
                logging.info(
                    'free=%s%%, written=%s, elapsed=%0.3f sec, throughput=%s/s', du.percent,
                    bytes_to_size(bytes_written), elapsed, bytes_to_size(throughput)
                )
                prior_bytes = bytes_written

    end = time.time()
    bytes_written = os.path.getsize(data_filepath)
    elapsed = end - start
    throughput = 0.0
    if elapsed > 0:
        throughput = bytes_written / elapsed
    du = psutil.disk_usage(drive_letter)
    logging.info(
        'free=%s%%, written=%s, elapsed=%0.3f sec, throughput=%s/s', du.percent, bytes_to_size(bytes_written), elapsed,
        bytes_to_size(throughput)
    )

    if not no_delete:
        logging.warning('removing data_filepath "%s"', data_filepath)
        os.remove(data_filepath)
    return bytes_written, elapsed, byte_array


def write_fulpak(
    byte_array=None,
    data_filepath=con.DATA_FILEPATH,
    size=con.SIZE,
    value=con.VALUE,
    chunk_size=con.CHUNK_SIZE,
    log_every=con.LOG_EVERY,
    no_cheat=con.NO_CHEAT,
    no_delete=con.NO_DELETE,
    stop_event=con.STOP_EVENT,
    **kwargs
):
    # type: (Optional[bytearray], str, int, int, int, int, bool, bool, threading.Event, Any) -> Tuple[int, float, bytearray]  # noqa: E501
    '''
    Description:
        Optional bytearray, write it to the disk repeatedly until the disk screams it can't anymore

    Arguments:
        byte_array: Optional[bytearray]
            use this bytearray and write to data_filepath
        data_filepath: str
            the destination of the actual file to be written since we're operating at the OS level
        size: int
            -1 to auto-determine by testing a few sizes, else, size in in bytes to repeat or burnin
        value: int
            -1 for random, else, [0,255] repeat the same value for all bytes
        chunk_size: int
            default 1MB, MUST evenly divide byte_array length
                instead of sequentially asserting, randomly dart around the file
                say the byte_array length 32, data_filepath length 64, chunk_size 4
                we will generate 64 / 4 = 16 "windows" to jump around and compare
        log_every: int
            default 1GB, log a progress report every X bytes
        no_cheat: bool
            default False, if True, dont apply this one neat trick
            if size > 1MB, simply repeat 1MB until size is filled up
        no_delete: bool
            default False, opt out of self-cleanup
        stop_event: threading.Event
            a way to short circuit exit if stop_event.is_set()
        **kwargs: varkwarguments

    Returns:
        Tuple[int, float, bytearray]
            bytes operated, elapsed in seconds, byte_array
    '''
    byte_array = get_byte_array(
        byte_array=byte_array,
        data_filepath=data_filepath,
        size=size,
        value=value,
        no_cheat=no_cheat,
        stop_event=stop_event
    )
    if not isinstance(byte_array, bytearray):
        raise TypeError(f'byte_array must be of type bytearray, provided {type(byte_array)}!')
    logging.debug('byte_array=%s, data_filepath="%s"', bytes_to_size(len(byte_array)), data_filepath)
    logging.info('write_fulpak with byte_array of %s', bytes_to_size(len(byte_array)))

    # write the bulk of the data
    drive_letter, _ = os.path.splitdrive(data_filepath)
    size = len(byte_array)
    prior_bytes = 0
    bytes_written = 0
    touch(data_filepath)
    start = time.time()
    with open(data_filepath, 'ab') as wb:
        while psutil.disk_usage(drive_letter).free > size:
            for i in range(0, len(byte_array), chunk_size):
                if stop_event.is_set():
                    break
                bytes_written += wb.write(byte_array[i:i + chunk_size])
                if bytes_written > prior_bytes + log_every:
                    end = time.time()
                    elapsed = end - start
                    if elapsed > 0:
                        throughput = bytes_written / elapsed
                    du = psutil.disk_usage(drive_letter)
                    logging.info(
                        'free=%s%%, written=%s, elapsed=%0.3f sec, throughput=%s/s', du.percent,
                        bytes_to_size(bytes_written), elapsed, bytes_to_size(throughput)
                    )
                    prior_bytes = bytes_written

        try:
            # write the last chunk in 1mb increments until disk fills and raises OSError
            for i in range(size // con.MB):
                if stop_event.is_set():
                    break
                if bytes_written > prior_bytes + log_every:
                    end = time.time()
                    elapsed = end - start
                    if elapsed > 0:
                        throughput = bytes_written / elapsed
                    du = psutil.disk_usage(drive_letter)
                    logging.info(
                        'free=%s%%, written=%s, elapsed=%0.3f sec, throughput=%s/s', du.percent,
                        bytes_to_size(bytes_written), elapsed, bytes_to_size(throughput)
                    )
                    prior_bytes = bytes_written

                if psutil.disk_usage(drive_letter).free > con.MB:
                    one_mb_array = byte_array[i * con.MB:(i + 1) * con.MB]
                    bytes_written += wb.write(one_mb_array)
                else:
                    break
        except OSError:
            pass  # this is expected behavior

    end = time.time()
    bytes_written = os.path.getsize(data_filepath)
    elapsed = end - start
    throughput = 0.0
    if elapsed > 0:
        throughput = bytes_written / elapsed
    du = psutil.disk_usage(drive_letter)
    logging.info(
        'free=%s%%, written=%s, elapsed=%0.3f sec, throughput=%s/s', du.percent, bytes_to_size(bytes_written), elapsed,
        bytes_to_size(throughput)
    )

    if not no_delete:
        logging.warning('removing data_filepath "%s"', data_filepath)
        os.remove(data_filepath)
    return bytes_written, elapsed, byte_array


def read_seq(
    byte_array=None,
    data_filepath=con.DATA_FILEPATH,
    size=con.SIZE,
    value=con.VALUE,
    chunk_size=con.CHUNK_SIZE,
    log_every=con.LOG_EVERY,
    no_cheat=con.NO_CHEAT,
    stop_event=con.STOP_EVENT,
    **kwargs
):
    # type: (Optional[bytearray], str, int, int, int, int, bool, threading.Event, Any) -> Tuple[int, float, bytearray]  # noqa: E501
    '''
    Description:
        Write a file to the disk, perhaps random, repeatedly, fill the drive, set size, etc.

        >>> write('/tmp/file')  # write random file to fill the disk, loop indefinitely
        >>> write('/tmp/file', duration=10)  # write random file to fill the disk, break if elapsed exceeded, else loop
        >>> write('/tmp/file', value=69, size=1024)  # write 1024 bytes of 64 to the same file infinitely (burn-in)

    Arguments:
        byte_array: Optional[bytearray]
            use this bytearray instead of generating it (useful when passing one to the other)
        data_filepath: str
            the destination of the actual file to be written since we're operating at the OS level
        size: int
            -1 to auto-determine by testing a few sizes, else, size in in bytes to repeat or burnin
        value: int
            -1 for random, else, [0,255] repeat the same value for all bytes
        chunk_size: int
            default 1MB, MUST evenly divide byte_array length
                instead of sequentially asserting, randomly dart around the file
                say the byte_array length 32, data_filepath length 64, chunk_size 4
                we will generate 64 / 4 = 16 "windows" to jump around and compare
        log_every: int
            default 1GB, log a progress report every X bytes
        no_cheat: bool
            default False, if True, dont apply this one neat trick
            if size > 1MB, simply repeat 1MB until size is filled up
        stop_event: threading.Event
            a way to short circuit exit if stop_event.is_set()
        **kwargs: varkwarguments

    Returns:
        Tuple[int, float, bytearray]
            bytes operated, elapsed in seconds, byte_array
    '''
    byte_array = get_byte_array(
        byte_array=byte_array,
        data_filepath=data_filepath,
        size=size,
        value=value,
        no_cheat=no_cheat,
        stop_event=stop_event
    )
    if not isinstance(byte_array, bytearray):
        raise TypeError(f'byte_array must be of type bytearray, provided {type(byte_array)}!')
    logging.debug(
        'byte_array=%s, data_filepath="%s", chunk_size=%s', bytes_to_size(len(byte_array)), data_filepath,
        bytes_to_size(chunk_size)
    )
    logging.info(
        'read_seq with byte_array of %s, chunk_size=%s', bytes_to_size(len(byte_array)), bytes_to_size(chunk_size)
    )

    drive_letter, _ = os.path.splitdrive(data_filepath)
    bytes_read = 0
    prior_bytes = 0
    start = time.time()
    iiteration = 0
    with open(data_filepath, 'rb') as rb:
        read_array = rb.read(chunk_size)
        bytes_read += len(read_array)
        while read_array:
            if stop_event.is_set():
                break
            if bytes_read > prior_bytes + log_every:
                end = time.time()
                elapsed = end - start
                if elapsed > 0:
                    throughput = bytes_read / elapsed
                du = psutil.disk_usage(drive_letter)
                logging.info(
                    'free=%s%%, read=%s, elapsed=%0.3f sec, throughput=%s/s', du.percent, bytes_to_size(bytes_read),
                    elapsed, bytes_to_size(throughput)
                )
                prior_bytes = bytes_read

            iiteration += 1
            if len(read_array) == len(byte_array):
                assert read_array == byte_array, (
                    '\n'.join(
                        [f'on iteration {iiteration}, full array read != write!'] + diff_bytes(read_array, byte_array)
                    )
                )
            else:
                sub_array = byte_array[:len(read_array)]
                assert read_array == sub_array, (
                    '\n'.join(
                        [f'on iteration {iiteration}, sub full array read != write!'] +
                        diff_bytes(read_array, sub_array)
                    )
                )

            read_array = rb.read(chunk_size)
            bytes_read += len(read_array)

    end = time.time()
    elapsed = end - start
    throughput = 0.0
    if elapsed > 0:
        throughput = bytes_read / elapsed
    du = psutil.disk_usage(drive_letter)
    logging.info(
        'free=%s%%, read=%s, elapsed=%0.3f sec, throughput=%s/s', du.percent, bytes_to_size(bytes_read), elapsed,
        bytes_to_size(throughput)
    )

    return bytes_read, throughput, byte_array


def read_rand(
    byte_array=None,
    data_filepath=con.DATA_FILEPATH,
    size=con.SIZE,
    value=con.VALUE,
    log_every=con.LOG_EVERY,
    no_cheat=con.NO_CHEAT,
    chunk_size=con.CHUNK_SIZE,
    stop_event=con.STOP_EVENT,
    **kwargs
):
    # type: (Optional[bytearray], str, int, int, int, bool, int, threading.Event, Any) -> Tuple[int, float, bytearray]  # noqa: E501
    '''
    Description:
        Read a file by randomly jumping around with seek and reads

    Arguments:
        byte_array: Optional[bytearray]
            use this bytearray instead of generating it (useful when passing one to the other)
        data_filepath: str
            the destination of the actual file to be written since we're operating at the OS level
        size: int
            -1 to auto-determine by testing a few sizes, else, size in in bytes to repeat or burnin
        value: int
            -1 for random, else, [0,255] repeat the same value for all bytes
        log_every: int
            default 1GB, log a progress report every X bytes
        no_cheat: bool
            default False, if True, dont apply this one neat trick
            if size > 1MB, simply repeat 1MB until size is filled up
        chunk_size: int
            default 1MB, MUST evenly divide byte_array length
                instead of sequentially asserting, randomly dart around the file
                say the byte_array length 32, data_filepath length 64, chunk_size 4
                we will generate 64 / 4 = 16 "windows" to jump around and compare
        stop_event: threading.Event
            a way to short circuit exit if stop_event.is_set()
        **kwargs: varkwarguments

    Returns:
        Tuple[int, float, bytearray]
            bytes operated, elapsed in seconds, byte_array
    '''
    byte_array = get_byte_array(
        byte_array=byte_array,
        data_filepath=data_filepath,
        size=size,
        value=value,
        no_cheat=no_cheat,
        stop_event=stop_event
    )
    if not isinstance(byte_array, bytearray):
        raise TypeError(f'byte_array must be of type bytearray, provided {type(byte_array)}!')
    logging.debug('byte_array=%s, data_filepath="%s"', bytes_to_size(len(byte_array)), data_filepath)
    logging.info(
        'read_rand with byte_array of %s and chunk_size %s', bytes_to_size(len(byte_array)), bytes_to_size(chunk_size)
    )

    drive_letter, _ = os.path.splitdrive(data_filepath)
    filesize = os.path.getsize(data_filepath)
    arrsize = len(byte_array)
    if arrsize % chunk_size != 0:
        raise TypeError(
            f'chunk_size must evenly divide the byte_array size! {arrsize} % {chunk_size} == {arrsize % chunk_size}!'
        )
    # [0, 640, 1280, 1920, 2560, 3200, 3840, 4480, 5120, 5760, 6400, 7040, 7680, 8320, 8960, 9600]
    idxes = list(range(0, filesize, chunk_size))
    # [3840, 9600, 1920, 640, 5760, 4480, 2560, 3200, 5120, 7680, 7040, 6400, 8320, 0, 8960, 1280]
    random.shuffle(idxes)
    bytes_read = 0
    start = time.time()
    prior_bytes = 0
    i_divs = int(math.log10(len(idxes))) - 1
    if i_divs < 0:
        i_divs = 0
    i_divs = 10**i_divs * 5
    with open(data_filepath, 'rb') as rb:
        for i, file_idx in enumerate(idxes):
            if stop_event.is_set():
                break
            if bytes_read > prior_bytes + log_every:
                end = time.time()
                elapsed = end - start
                if elapsed > 0:
                    throughput = bytes_read / elapsed
                du = psutil.disk_usage(drive_letter)
                logging.info(
                    'free=%s%%, read=%s, elapsed=%0.3f sec, throughput=%s/s', du.percent, bytes_to_size(bytes_read),
                    elapsed, bytes_to_size(throughput)
                )
                prior_bytes = bytes_read
            # if i % i_divs == 0:  # this also works very well TODO: good idiom to have
            #     logging.debug('chunk %s / %s', i + 1, len(idxes))

            _ = rb.seek(file_idx)
            read_array = rb.read(chunk_size)
            bytes_read += len(read_array)

            truth_idx = file_idx % arrsize
            # in case we're at the LAST idx, and didnt read much
            truth_array = byte_array[truth_idx:truth_idx + len(read_array)]
            assert read_array == truth_array, (
                '\n'.join([f'on iteration {i}, full array read != write!'] + diff_bytes(read_array, truth_array))
            )

    end = time.time()
    elapsed = end - start
    throughput = 0.0
    if elapsed > 0:
        throughput = bytes_read / elapsed
    du = psutil.disk_usage(drive_letter)
    logging.info(
        'free=%s%%, read=%s, elapsed=%0.3f sec, throughput=%s/s', du.percent, bytes_to_size(bytes_read), elapsed,
        bytes_to_size(throughput)
    )

    return bytes_read, elapsed, byte_array
