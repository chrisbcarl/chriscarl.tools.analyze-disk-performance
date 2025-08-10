# stdlib
import __main__
import os
import sys
import time
import logging
import datetime
import subprocess
import threading  # noqa: F401
from typing import Any, List, Optional  # noqa: F401

# third party

# app
import constants
import system
from stdlib import abspath

SCRIPT_DIRPATH = os.path.abspath(os.path.dirname(__file__))


def health(
    # delete
    ignore_partitions=constants.IGNORE_PARTITIONS,
    include_partitions=None,
    # array
    size=4 * constants.MB,
    value=constants.VALUE,
    # flow
    iterations=3,
    duration=constants.DURATION,
    # read
    chunk_size=constants.CHUNK_SIZE,
    # general/telemetry
    poll=150.0,
    log_level=constants.LOG_LEVEL,
    log_every=64 * constants.GB,
    stop_event=constants.STOP_EVENT,
    **kwargs
):
    # type: (List[str], Optional[List[str]], int, int, int, float|int, int, float|int, str, int, threading.Event, Any) -> None  # noqa: E501
    '''
    Description:
        Launch a pre-determined flow upon every relevant disk. WARNING: DO NOT RUN IN A HIGHLY POPULATED PC!

    Arguments:
        ignore_partitions: List[str]
            Ex) ['C']
            If you know of partitions youd like to avoid ahead of time, maybe avoid deleting them...
        include_partitions: Optional[List[str]]
            If you know which to delete, delete only those, override ignore_partitions
        size: int
            -1 to auto-determine by testing a few sizes, else, size in in bytes to repeat or burnin
        value: int
            -1 for random, else, repeat the same value for all bytes
        iterations: int
            -1 for loop infinitely, else exec ends iteration exceeded (or duration exceeded)
        duration: float|int
            -1 for loop infinitely, else exec ends after elapsed exceeded in seconds (or duration exceeded)
        chunk_size: int
            default 1MB, MUST evenly divide byte_array length
                instead of sequentially asserting, randomly dart around the file
                say the byte_array length 32, data_filepath length 64, chunk_size 4
                we will generate 64 / 4 = 16 "windows" to jump around and compare
        poll: float|int
            interval between sampling
        log_every: int
            default 1GB, log a progress report every X bytes
        stop_event: threading.Event
            a way to short circuit exit if stop_event.is_set()

    Returns:
        None
    '''
    operation = 'health'

    if system.admin_detect() != 0:
        raise RuntimeError('Must be run as administrator or sudo!')

    logging.info('deleting partitions...')
    disk_numbers = system.delete_partitions(ignore_partitions=ignore_partitions, include_partitions=include_partitions)

    logging.info('creating partitions...')
    disk_number_to_letter_dict = system.create_partitions(disk_numbers=disk_numbers)
    if disk_numbers and (len(disk_numbers) != len(disk_number_to_letter_dict)):
        raise RuntimeError(
            f'Number of disks != number of partitions created! '
            f'{len(disk_numbers)} != {len(disk_number_to_letter_dict)} | {disk_number_to_letter_dict}'
        )

    popens = []
    logging.info('flowing on %d partitions/drives...', len(disk_number_to_letter_dict))
    started = datetime.datetime.now()
    output_dirpath = constants.TEMP_DIRPATH
    for drive_number, drive_letter in disk_number_to_letter_dict.items():
        data_filepath = abspath(f'{drive_letter}:/{drive_number}-{operation}.dat')
        stdout = abspath(f'{output_dirpath}/{drive_number}-{operation}.stdout')
        cmd = [
            sys.executable,
            __main__.__file__,
            # flow control
            'flow',
            '--steps',
            'write_fulpak',
            'read_seq',
            '--flow-iterations',
            iterations,
            '--flow-duration',
            duration,
            # general / telemetry
            '--no-telemetry',
            '--data-filepath',
            data_filepath,
            '--log-level',
            log_level,
            '--log-every',
            log_every,
            # array
            # read/write args
            '--iterations',
            1
        ]

        # array
        if size != constants.SIZE:
            cmd += ['--size', size]
        if value != constants.VALUE:
            cmd += ['--value', value]
        if chunk_size != constants.CHUNK_SIZE:
            cmd += ['--chunk-size', chunk_size]

        cmd_strs = [str(ele) for ele in cmd]
        logging.debug('drive %s (%s): %s', drive_number, drive_letter, subprocess.list2cmdline(cmd_strs))
        with open(stdout, 'wb') as sout:
            popen = subprocess.Popen(cmd_strs, stdout=sout)
            popens.append(popen)
    exit_codes = [-1] * len(popens)
    # pids = [popen.pid for popen in popens]

    try:
        while True:
            now = datetime.datetime.now()
            logging.info('elapsed: %s', now - started)
            for p, popen in enumerate(popens):
                exit_code = popen.poll()
                if isinstance(exit_code, int):
                    exit_codes[p] = exit_code

            if all([ele != -1 for ele in exit_codes]):
                logging.info('All %r finished!', operation)
                break
            time.sleep(poll)
    except KeyboardInterrupt:
        logging.warning('ctrl + c detected! killing processes, removing resources...')
        logging.debug('ctrl + c detected! killing processes, removing resources...', exc_info=True)

    stop_event.set()
    for popen in popens:
        if popen.poll() is None:
            popen.kill()
            subprocess.Popen(['taskkill', '/pid', str(popen.pid), '/f', '/t'], shell=True).wait()
    for drive_number, drive_letter in disk_number_to_letter_dict.items():
        data_filepath = abspath(f'{drive_letter}:/{drive_number}-{operation}.dat')
        if os.path.isfile(data_filepath):
            try:
                os.remove(data_filepath)
            except Exception:
                logging.error('unable to delete ""%s', data_filepath, exc_info=True)

    logging.info('removing partitions...')
    system.delete_partitions(include_partitions=list(disk_number_to_letter_dict.values()))

    logging.info('closing resources...')
    failures = [exit_code != 0 for exit_code in exit_codes]
    if failures:
        logging.error('Failed! %d / %d processes failed with exit codes: %s!', len(failures), len(popens), failures)
