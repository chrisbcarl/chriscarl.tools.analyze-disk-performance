# stdlib
import os
import sys
import datetime

APP_NAME = 'chriscarl.tools.analyze-disk-performance'
NOW = datetime.datetime.now()
TEMP_DIRPATH = f'C:/temp/{APP_NAME}' if sys.platform == 'win32' else f'/tmp/{APP_NAME}'
TEMP_DIRPATH = os.path.join(TEMP_DIRPATH, NOW.strftime('%Y%m%d-%H%M'))  # %S
TEMP_DIRPATH = os.path.abspath(TEMP_DIRPATH)
DRIVE, _ = os.path.splitdrive(TEMP_DIRPATH)
DATA_FILEPATH = os.path.join(TEMP_DIRPATH, 'data.dat')
PERF_FILEPATH = os.path.join(TEMP_DIRPATH, 'performance.csv')
BYTE_ARRAY_THROUGHPUT_FILEPATH = os.path.join(TEMP_DIRPATH, 'byte_array_throughput.csv')
OPERATIONS = ['perf', 'fill', 'perf+fill', 'loop', 'write', 'perf+write', 'health', 'perf+fill+read', 'smartmon']
VALUE = -1
DURATION = 3
ITERATIONS = 5
CRYSTALDISKINFO_EXE = 'DiskInfo64.exe' if sys.platform == 'win32' else 'DiskInfo64'
CRYSTALDISKINFO_TXT = ''
