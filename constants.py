# stdlib
import os
import sys
import logging
import datetime
import multiprocessing

APP_NAME = 'chriscarl.tools.analyze-disk-performance'
NOW = datetime.datetime.now()
TEMP_DIRPATH = f'C:/temp/{APP_NAME}' if sys.platform == 'win32' else f'/tmp/{APP_NAME}'
TEMP_DIRPATH = os.path.join(TEMP_DIRPATH, NOW.strftime('%Y%m%d-%H%M'))  # %S
TEMP_DIRPATH = os.path.abspath(TEMP_DIRPATH)
DRIVE, _ = os.path.splitdrive(TEMP_DIRPATH)
DATA_FILEPATH = os.path.join(TEMP_DIRPATH, 'data.dat')
PERF_FILEPATH = os.path.join(TEMP_DIRPATH, 'performance.csv')
SEARCH_OPTIMAL_FILEPATH = os.path.join(TEMP_DIRPATH, 'search_optimal_filepath.csv')
SUMMARY_FILEPATH = os.path.join(TEMP_DIRPATH, 'summary.csv')
SMART_FILEPATH = os.path.join(TEMP_DIRPATH, 'smart.csv')
VALUE = -1
DURATION = -1
ITERATIONS = -1
FLOW_DURATION = -1
FLOW_ITERATIONS = 3
SIZE = 1
POLL = 15
RANDOM_READ = -1
BURN_IN = False
SEARCH_OPTIMAL = True
NO_CRYSTALDISKINFO = False
ALL_DRIVES = False
KB = 1024**1
MB = 1024**2
GB = 1024**3
CRYSTALDISKINFO_EXE = 'DiskInfo64.exe' if sys.platform == 'win32' else 'DiskInfo64'
CRYSTALDISKINFO_TXT = ''
CRYSTAL_KEYS = [
    'Health Status',
    'Disk Number',
    'Model',
    'Serial Number',
    'Disk Size',
    'Power On Count',
    'Host Reads',
    'Host Writes',
    # sabrent
    'End to End Error Detection Count',
    'Uncorrectable Error Count',
    # intel
    'Media and Data Integrity Errors',
    'Number of Error Information Log Entries'
    # sandisk
    'End-to-End Error Detection/Correction Count',
    'Reported Uncorrectable Errors'
]

OPERATIONS = ['perf', 'fill', 'perf+fill', 'loop', 'write', 'perf+write', 'health', 'perf+fill+read', 'smartmon']
LOG_LEVELS = list(logging._nameToLevel)  # pylint: disable=(protected-access)
LOG_LEVEL = 'INFO'

# arg defaults
CPU_COUNT = multiprocessing.cpu_count()
