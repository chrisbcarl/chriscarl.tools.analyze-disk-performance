# stdlib
import os
import sys
import time
import logging
import datetime
import subprocess
from typing import Any, Tuple, Optional  # noqa: F401

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
    size=constants.SIZE,
    value=constants.VALUE,
    # flow
    iterations=3,
    duration=constants.DURATION,
    # write
    burn_in=constants.BURN_IN,
    # read
    chunk_size=constants.CHUNK_SIZE,
    # general/telemetry
    log_level=constants.LOG_LEVEL,
    log_every=constants.LOG_EVERY,
    stop_event=constants.STOP_EVENT,
    poll=constants.POLL,
):
    '''
    Description:
        Launch a pre-determined flow upon every relevant disk. WARNING: DO NOT RUN IN HIGHLY POPULATED PCs!

    Arguments:
        log_unit: str
            default GB, frequency of i/o that gets logged by units
        log_mod: int
            default 8, frequency of i/o that gets logged by EVERY OTHER units
    '''
    operation = 'write+read'
    logging.info('starting %r', operation)

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
            abspath(__file__),
            # flow control
            'flow',
            '--steps',
            'write',
            'read',
            '--flow-iterations',
            iterations,
            '--flow-duration',
            duration,
            # general / telemetry
            '--skip-telemetry',
            '--data-filepath',
            data_filepath,
            '--log-level',
            log_level,
            # array
            # read/write args
            '--iterations',
            1
        ]

        # array
        if size == constants.SIZE:
            cmd += ['--search-optimal']
        else:
            cmd += ['--size', size]
        if value != constants.VALUE:
            cmd += ['--value', value]

        if burn_in:
            cmd += ['--burn-in']
        if chunk_size != constants.chunk_size:
            cmd += ['--random-read', chunk_size]
        cmd = [str(ele) for ele in cmd]
        logging.debug('drive %s (%s): %s', drive_number, drive_letter, subprocess.list2cmdline(cmd))
        with open(stdout, 'wb') as sout:
            popen = subprocess.Popen(cmd, stdout=sout)
            popens.append(popen)

    try:
        while True:
            now = datetime.datetime.now()
            logging.info('elapsed: %s', now - started)
            if all([popen.poll() is not None for popen in popens]):
                logging.info('All %r finished!', operation)
                break
            time.sleep(poll)
    except KeyboardInterrupt:
        logging.warning('ctrl + c detected! killing processes, removing resources...')
        logging.debug('ctrl + c detected! killing processes, removing resources...', exc_info=True)

    logging.info('closing resources...')
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
