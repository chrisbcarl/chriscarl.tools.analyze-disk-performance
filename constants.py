# stdlib
import os
import sys
import logging
import datetime
import threading
import multiprocessing
from typing import List  # noqa: F401

APP_NAME = 'chriscarl.tools.analyze-disk-performance'
NOW = datetime.datetime.now()
TEMP_DIRPATH = f'C:/temp/{APP_NAME}' if sys.platform == 'win32' else f'/tmp/{APP_NAME}'
TEMP_DIRPATH = os.path.join(TEMP_DIRPATH, NOW.strftime('%Y%m%d-%H%M'))  # %S
TEMP_DIRPATH = os.path.abspath(TEMP_DIRPATH)
DRIVE, _ = os.path.splitdrive(TEMP_DIRPATH)
DATA_FILEPATH = os.path.join(TEMP_DIRPATH, 'data.dat')
PERF_FILEPATH = os.path.join(TEMP_DIRPATH, 'performance.csv')
SUMMARY_FILEPATH = os.path.join(TEMP_DIRPATH, 'summary.csv')
SMART_FILEPATH = os.path.join(TEMP_DIRPATH, 'smart.csv')

KB = 1024**1
MB = 1024**2
GB = 1024**3

VALUE = -1
DURATION = -1
ITERATIONS = -1
FLOW_DURATION = -1
FLOW_ITERATIONS = 3
SIZE = -1
POLL = 15
CHUNK_SIZE = KB
NO_CHEAT = False
BURN_IN = False
SEARCH_OPTIMAL = False
NO_CRYSTALDISKINFO = False
ALL_DRIVES = False
NO_TELEMETRY = False
NO_ADMIN = False
NO_DELETE = False
# by default none, its too dangerous to set a partition to create without information
DISK_NUMBERS = []  # type: List[str|int]
# DEFAULTS = {
#     'VALUE': VALUE,
#     'DURATION': DURATION,
#     'ITERATIONS': ITERATIONS,
#     'FLOW_DURATION': FLOW_DURATION,
#     'FLOW_ITERATIONS': FLOW_ITERATIONS,
#     'SIZE': SIZE,
#     'POLL': POLL,
#     'CHUNK_SIZE': CHUNK_SIZE,
#     'BURN_IN': BURN_IN,
#     'SEARCH_OPTIMAL': SEARCH_OPTIMAL,
#     'NO_CRYSTALDISKINFO': NO_CRYSTALDISKINFO,
#     'ALL_DRIVES': ALL_DRIVES,
#     'NO_TELEMETRY': NO_TELEMETRY,
# }
# DEFAULTS.update({k.lower(): DEFAULTS[k] for k in list(DEFAULTS.keys())})

LOG_LEVELS = list(logging._nameToLevel)  # pylint: disable=(protected-access)
LOG_LEVEL = 'INFO'

LOG_EVERY = 4 * GB

CRYSTALDISKINFO_EXE = 'DiskInfo64.exe' if sys.platform == 'win32' else 'DiskInfo64'
CRYSTALDISKINFO_TXT = ''
CRYSTAL_ERROR_KEYS = [
    # sabrent
    'End to End Error Detection Count',
    'Uncorrectable Error Count',
    # intel
    'Media and Data Integrity Errors',
    'Number of Error Information Log Entries'
    # sandisk
    'End-to-End Error Detection/Correction Count',
    'Reported Uncorrectable Errors',
]
CRYSTAL_KEYS = [
    'Health Status',
    'Disk Number',
    'Model',
    'Serial Number',
    'Disk Size',
    'Transfer Mode',
    'Power On Count',
    'Host Reads',
    'Host Writes',
] + CRYSTAL_ERROR_KEYS
IGNORE_PARTITIONS = ['A', 'B', 'C']

OPERATIONS = ['perf', 'fill', 'perf+fill', 'loop', 'write', 'perf+write', 'health', 'perf+fill+read', 'smartmon']

WRITE_MODES = ['burnin', 'fulpak']
WRITE_MODE = WRITE_MODES[0]

# arg defaults
CPU_COUNT = multiprocessing.cpu_count()
STOP_EVENT = threading.Event()
