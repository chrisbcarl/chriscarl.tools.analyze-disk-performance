# stdlib
import os
import json
import logging
import subprocess
from typing import List, Optional, Dict  # noqa: F401

# third party
from stdlib import abspath
import constants

SCRIPT_DIRPATH = os.path.abspath(os.path.dirname(__file__))


def admin_detect():
    # type: () -> int
    try:
        admin_ps1 = abspath(SCRIPT_DIRPATH, r"scripts\win32\admin.ps1")
        subprocess.check_call(['powershell', admin_ps1])
        logging.info('admin detected!')
    except subprocess.CalledProcessError:
        logging.warning('not admin!')
        return 1
    return 0


def delete_partitions(ignore_partitions=constants.IGNORE_PARTITIONS, include_partitions=None):
    # type: (List[str], Optional[List[str]]) -> List[str]
    '''
    Description:
        Look through all partitions and remove them so the disks are raw

    Arguments:
        ignore_partitions: List[str]
            Ex) ['C']
            If you know of partitions youd like to avoid ahead of time, maybe avoid deleting them...
        include_partitions: List[str]
            If you know which to delete, delete only those, override ignore_partitions

    Returns:
        List[str]
            list of disk numbers as readable by CrystalDiskMark and Windows Disk Utility
    '''
    if admin_detect() != 0:
        raise RuntimeError('Must be run as administrator or sudo!')

    disk_numbers = []
    if not include_partitions:
        # get all partitions
        read_partitions_ps1 = abspath(SCRIPT_DIRPATH, r"scripts\win32\read-partitions.ps1")
        cmd = ['powershell', read_partitions_ps1]
        logging.debug(subprocess.list2cmdline(cmd))
        output = subprocess.check_output(cmd, universal_newlines=True)
        read_partitions = json.loads(output)
        logging.info('partitions identified: %s', json.dumps(read_partitions, indent=2))

        # filter out all partitions that dont belong
        include_partitions = [key for key in read_partitions if key not in ignore_partitions]
        logging.debug('removing drive letters: %s', include_partitions)
        disk_numbers = [val['DiskNumber'] for key, val in read_partitions.items() if key not in ignore_partitions]
        logging.info('disk numbers to be removed after filtering %s: %s', ignore_partitions, disk_numbers)

    if include_partitions:
        # remove partitions so they return to raw
        delete_partitions_ps1 = abspath(SCRIPT_DIRPATH, r"scripts\win32\delete-partitions.ps1")
        cmd = ['powershell', delete_partitions_ps1, '-DriveLetters', ','.join(include_partitions)]
        logging.debug(subprocess.list2cmdline(cmd))
        output = subprocess.check_output(cmd, universal_newlines=True)
        logging.debug(output)
    if disk_numbers:
        logging.info('deleted partitions. disk numbers: %s', disk_numbers)
        return disk_numbers

    return disk_numbers


def create_partitions(disk_numbers=constants.DISK_NUMBERS):
    # type: (List[str|int]) -> Dict[str, str]
    '''
    Description:
        Go through any raw disk and give them a partition, returning a dict of number: letter

    Arguments:
        disk_numbers: List[int]
            Ex) [0]
            If you know the disk numbers ahead of time, provide them
            else, any raw disk will be partitioned and assigned a drive letter

    Returns:
        Dict[str, str]
            dict of number: letter
            ex) {"1": "D:"}
    '''
    if admin_detect() != 0:
        raise RuntimeError('Must be run as administrator or sudo!')

    # scan for RAW and make new partitions
    create_partitions_ps1 = abspath(SCRIPT_DIRPATH, r"scripts\win32\create-partitions.ps1")
    cmd = ['powershell', create_partitions_ps1]
    if disk_numbers:
        cmd += ['-DiskNumbers', ','.join(str(ele) for ele in disk_numbers)]
    logging.debug(subprocess.list2cmdline(cmd))
    output = subprocess.check_output(cmd, universal_newlines=True)
    create_partitions_sentinel = "Begin Output Parsing Here:"
    logging.debug(output)
    output = output[output.find(create_partitions_sentinel) + len(create_partitions_sentinel) + 1:].strip()
    logging.debug(output)
    disk_number_to_letter_dict = json.loads(output)
    logging.info('created new partitions: %s!', disk_number_to_letter_dict)
    for k, v in disk_number_to_letter_dict.items():
        if v is None:
            raise RuntimeError(f'Drive {k} was unable to create a partition: {v}!')

    return disk_number_to_letter_dict
