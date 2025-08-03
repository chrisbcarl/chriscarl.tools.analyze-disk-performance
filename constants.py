# stdlib
import os
import sys
import datetime

NOW = datetime.datetime.now()
TEMP_DIRPATH = '/temp' if sys.platform == 'win32' else '/tmp'
TEMP_DIRPATH = os.path.join(TEMP_DIRPATH, NOW.strftime('%Y%m%d-%H%M'))  # %S
TEMP_DIRPATH = os.path.abspath(TEMP_DIRPATH)
DRIVE, _ = os.path.splitdrive(TEMP_DIRPATH)
DATA_FILEPATH = os.path.join(TEMP_DIRPATH, 'data.dat')
PERF_FILEPATH = os.path.join(TEMP_DIRPATH, 'perf.csv')
OPERATIONS = ['perf', 'fill', 'perf+fill', 'loop', 'write', 'perf+write', 'health', 'perf+fill+read', 'smartmon']
VALUE = -1
DURATION = 3
ITERATIONS = 5
CRYSTALDISKINFO_EXE = 'DiskInfo64.exe' if sys.platform == 'win32' else 'DiskInfo64'
CRYSTALDISKINFO_TXT = ''
