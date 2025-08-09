# stdlib
import os
import sys
import re
import csv
import time
import pprint
import logging
import datetime
import threading  # noqa: F401
import subprocess
from typing import Tuple, Dict, Optional  # noqa: F401

# third party
import numpy as np
import pandas as pd
import psutil

# app
import constants
import third
import system
from stdlib import abspath

SCRIPT_DIRPATH = os.path.abspath(os.path.dirname(__file__))


def summarize_crystaldiskinfo_df(df):
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['Read Perf'] = pd.Series([''] * len(df))
    df['Write Perf'] = pd.Series([''] * len(df))
    ilocs = []
    rows = []
    for grouping, group_df in df.groupby(['Serial Number']):
        dts = group_df['datetime']
        dt_min, dt_max = dts.min(), dts.max()
        iloc_min = dts.index[dts.argmin()]
        iloc_max = dts.index[dts.argmax()]
        ilocs += [iloc_min, iloc_max]
        elapsed = (dt_max - dt_min).total_seconds()

        reads = group_df['Host Reads']
        reads_min, reads_max = reads.min(), reads.max()
        read_throughput, write_throughput = '?', '?'
        if reads_min == np.nan:
            reads_min = -1
            reads_min = -1
        elif isinstance(reads_min, (int, float)):
            pass
        else:
            unit = reads_min.split()[-1]
            reads_min, reads_max = reads_min.split()[0], reads_max.split()[0]
        reads_min, reads_max = float(reads_min), float(reads_max)

        writes = group_df['Host Writes']
        writes_min, writes_max = writes.min(), writes.max()
        if writes_min == np.nan:
            writes_min = -1
            writes_max = -1
        elif isinstance(writes_min, (int, float)):
            pass
        else:
            writes_min, writes_max = writes_min.split()[0], writes_max.split()[0]
        writes_min, writes_max = float(writes_min), float(writes_max)

        if elapsed == 0:
            read_throughput = f'0 {unit}/s'
            write_throughput = f'0 {unit}/s'
        else:
            read_throughput = f'{(reads_max - reads_min) / elapsed:0.3f} {unit}/s'
            write_throughput = f'{(writes_max - writes_min) / elapsed:0.3f} {unit}/s'
        df.loc[group_df.index, 'Read Perf'] = read_throughput
        df.loc[group_df.index, 'Write Perf'] = write_throughput

        drive_letter = df.iloc[iloc_max]['Drive Letter']
        disk_size = df.iloc[iloc_max]['Disk Size'].split()
        disk_size, disk_unit = disk_size[0], disk_size[1]
        serial = df.iloc[iloc_max]['Serial Number']
        health = df.iloc[iloc_max]['Health Status']
        disk_number = df.iloc[iloc_max]['Disk Number']
        pcs = df.iloc[iloc_max]['Power On Count'].split()[0]
        xfer = df.iloc[iloc_max]['Transfer Mode'].split(' | ')[-1]  # pcie
        # text = (
        #     f'{serial} ({disk_size} {disk_unit}) | {disk_number} | {drive_letter} | '
        #     f'{health} | Reads: {read_throughput} | Writes: {read_throughput} | '
        # )
        row = dict(
            serial=serial,
            size=f'{disk_size} {disk_unit}',
            number=disk_number,
            letter=drive_letter,
            health=health,
            elapsed=f'{elapsed / 3600:0.2f}hrs',
            reads=f'{reads_max - reads_min} {disk_unit}',
            writes=f'{writes_max - writes_min} {disk_unit}',
            read_bw=read_throughput,
            write_bw=write_throughput,
            pcs=pcs,
            pcie=xfer,
        )
        for col in constants.CRYSTAL_ERROR_KEYS:
            if col not in df.columns:
                continue
            errs = group_df[col]
            err_min, err_max = errs.min(), errs.max()
            if pd.isna(err_min):
                err_min = 0
            if pd.isna(err_max):
                err_max = 0
            err_delta = err_max - err_min
            row[f'{col} (Delta)'] = err_delta

        rows.append(row)

    df = pd.DataFrame(rows)
    return df


def crystaldiskinfo_detect():
    # type: () -> int
    try:
        output = subprocess.check_output(
            ['where.exe' if sys.platform == 'win32' else 'which', constants.CRYSTALDISKINFO_EXE],
            universal_newlines=True
        )
        for line in output.splitlines():
            strip = line.strip()
            if os.path.isfile(strip):
                constants.CRYSTALDISKINFO_EXE = strip
                break
        if not os.path.isfile(strip):
            raise OSError(f'Could not find "{constants.CRYSTALDISKINFO_EXE}"!')
    except subprocess.CalledProcessError:
        logging.warning('CrystalDiskInfo not installed or not on path!')
        return 1
    return 0


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


def telemetry_smart(stop_event=constants.STOP_EVENT):
    # type: (threading.Event) -> Tuple[Dict[str, dict], dict]
    '''
    Basically smart can fail to detect drive letter stuff from time to time, best to wait a while...
    '''
    # Traceback (most recent call last):
    #   File "C:\Python312\Lib\threading.py", line 1075, in _bootstrap_inner
    #     self.run()
    #   File "C:\Python312\Lib\threading.py", line 1012, in run
    #     self._target(*self._args, **self._kwargs)
    #   File "X:\src\chriscarl.tools.analyze-disk-performance\app.py", line 127, in _telemetry
    #     disk_number = letter_map[drive_letter]
    #                   ~~~~~~~~~~^^^^^^^^^^^^^^
    while True:
        try:
            cdi = crystaldiskinfo()
            letter_map = {value['Drive Letter']: num for num, value in cdi.items()}
            return cdi, letter_map
        except Exception:
            logging.debug('error, trying again in 5 sec...', exc_info=True)
            for _ in range(int(5 * 100)):
                time.sleep(1 / 100)
                if stop_event.is_set():
                    break


def telemetry_async(
    no_telemetry=constants.NO_TELEMETRY,
    no_admin=constants.NO_ADMIN,
    no_crystaldiskinfo=constants.NO_CRYSTALDISKINFO,
    all_drives=constants.ALL_DRIVES,
    poll=constants.POLL,
    smart_filepath=constants.SMART_FILEPATH,
    data_filepath=constants.DATA_FILEPATH,
    summary_filepath=constants.SUMMARY_FILEPATH,
    stop_event=constants.STOP_EVENT,
):
    # type: (bool, bool, bool, bool, float|int, str, str, str, threading.Event) -> None
    '''
    Description:
        Poll telemetry including S.M.A.R.T. and others.

    Arguments:
        no_telemetry: bool
            short circuit exit
        poll: float|int
            interval between sampling
        data_filepath: str
            the destination of the actual file to be written since we're operating at the OS level
        smart_filepath: str
            where to save the crystaldisinfo S.M.A.R.T. data
        summary_filepath: str
            where to save the final executive summary
        no_crystaldiskinfo: bool
            default False, disable so you can run without admin
        all_drives: bool
            default False, get all drive S.M.A.R.T. data instead of only the drive who hosts the data_filepath

    Returns:
        bytearray
    '''
    if no_telemetry:
        logging.warning('skipping telemetry!')
        return

    logging.debug('polling every %s sec', poll)
    if all_drives:
        drive_letter = ''
    else:
        drive_letter, _ = os.path.splitdrive(data_filepath)

    prior_percent = None
    disk_number = ''
    prior_time = time.time()
    prior_reads = 0
    prior_writes = 0

    if not no_admin and not no_crystaldiskinfo:
        cdi, letter_map = telemetry_smart(stop_event=stop_event)
        logging.debug('letter_map:\n%s', pprint.pformat(letter_map, indent=2))
        if not cdi:
            # we failed and tried multiple times or things were cancelled early during a failure
            return
        cdi_df = pd.DataFrame(cdi.values())
        summary_columns = [key for key in constants.CRYSTAL_KEYS if key in cdi_df.columns]
        logging.debug('all S.M.A.R.T.:\n%s', cdi_df[summary_columns])
        if drive_letter:
            disk_number = letter_map[drive_letter]
        columns = third.upsert_df_to_csv(cdi_df, smart_filepath)

    logging.debug('disk_number: %s, drive_letter: %s', disk_number, drive_letter)
    logging.debug('no_admin: %s, no_crystaldiskinfo: %s', no_admin, no_crystaldiskinfo)
    iteration = 0
    while not stop_event.is_set():
        # logging.debug('poll: %d', iteration)
        stats = []
        if not no_admin and not no_crystaldiskinfo:
            cdi = crystaldiskinfo()
            with open(smart_filepath, 'a', encoding='utf-8', newline='') as a:
                writer = csv.DictWriter(a, fieldnames=columns)
                if disk_number:
                    value = cdi[str(disk_number)]
                    writer.writerow(value)

                    logging.debug(
                        'Disk %s (%s) S.M.A.R.T.\n%s', disk_number, drive_letter,
                        pd.DataFrame([{
                            k: v
                            for k, v in value.items() if k in summary_columns
                        }])
                    )

                    now = time.time()
                    elapsed = now - prior_time
                    prior_time = now

                    unit = 'GB'
                    host_reads = int(value['Host Reads'].split()[0])
                    host_writes = int(value['Host Writes'].split()[0])

                    read_throughput = (host_reads - prior_reads) / elapsed
                    write_throughput = (host_writes - prior_writes) / elapsed

                    if prior_reads != 0:
                        stats.append(
                            f'Read: {read_throughput:0.3f} {unit}/sec | Write: {write_throughput:0.3f} {unit}/sec'
                        )
                    prior_reads, prior_writes = host_reads, host_writes
                else:
                    for value in cdi.values():
                        writer.writerow(value)
                    logging.debug(
                        '\n%s',
                        pd.DataFrame(
                            [{
                                k: v
                                for k, v in value.items() if k in summary_columns
                            } for value in cdi.values()]
                        )
                    )

        if drive_letter and disk_number:
            du = psutil.disk_usage(drive_letter)
            if str(du.percent) != str(prior_percent):
                stats.append(f'Usage: {du.percent}%')
                prior_percent = str(du.percent)

            if stats:
                logging.info('Disk %s (%s) - %s', disk_number, drive_letter, ' - '.join(stats))

        iteration += 1
        for _ in range(int(poll * 25)):
            time.sleep(1 / 25)
            if stop_event.is_set():
                break

    if not no_admin and not no_crystaldiskinfo:
        cdi = crystaldiskinfo()
        if drive_letter and disk_number:
            df = pd.DataFrame([cdi[str(disk_number)]])
        else:
            df = pd.DataFrame(cdi.values())
        third.upsert_df_to_csv(df, smart_filepath)
        logging.info('S.M.A.R.T. Telemetry:\n%s', df.to_string(index=False))

        cdi_df = pd.read_csv(smart_filepath)

        summary_df = summarize_crystaldiskinfo_df(cdi_df)
        summary_df.to_csv(summary_filepath, index=False)


def telemetry(
    smart_filepath=constants.SMART_FILEPATH,
    data_filepath=constants.DATA_FILEPATH,
    summary_filepath=constants.SUMMARY_FILEPATH,
    no_admin=constants.NO_ADMIN,
    no_crystaldiskinfo=constants.NO_CRYSTALDISKINFO,
    all_drives=constants.ALL_DRIVES,
):
    # type: (str, str, str, bool, bool, bool) -> None
    '''
    Description:
        Poll telemetry including S.M.A.R.T. and others.

    Arguments:
        data_filepath: str
            the destination of the actual file to be written since we're operating at the OS level
        smart_filepath: str
            where to save the crystaldisinfo S.M.A.R.T. data
        summary_filepath: str
            where to save the final executive summary
        no_admin: bool
            default False, disable so you can skip the parts that require admin like crystaldiskinfo
        no_crystaldiskinfo: bool
            default False, disable so you can run without admin
        all_drives: bool
            default False, get all drive S.M.A.R.T. data instead of only the drive who hosts the data_filepath

    Returns:
        bytearray
    '''
    if all_drives:
        drive_letter = ''
    else:
        drive_letter, _ = os.path.splitdrive(data_filepath)

    disk_number = ''

    if not no_admin and not no_crystaldiskinfo:
        cdi, letter_map = telemetry_smart()
        logging.debug(pprint.pformat(letter_map, indent=2))
        logging.debug('letter_map:\n%s', pprint.pformat(letter_map, indent=2))
        if not cdi:
            # we failed and tried multiple times or things were cancelled early during a failure
            return
        cdi_df = pd.DataFrame(cdi.values())
        summary_columns = [key for key in constants.CRYSTAL_KEYS if key in cdi_df.columns]
        logging.debug('\n%s', cdi_df[summary_columns])
        if drive_letter:
            disk_number = letter_map[drive_letter]
        columns = third.upsert_df_to_csv(cdi_df, smart_filepath)

        with open(smart_filepath, 'a', encoding='utf-8', newline='') as a:
            writer = csv.DictWriter(a, fieldnames=columns)
            for value in cdi.values():
                writer.writerow(value)

        if drive_letter and disk_number:
            df = pd.DataFrame([cdi[str(disk_number)]])
        else:
            df = pd.DataFrame(cdi.values())
        logging.info('S.M.A.R.T. Telemetry:\n%s', df.to_string(index=False))

        summary_df = summarize_crystaldiskinfo_df(cdi_df)
        summary_df.to_csv(summary_filepath, index=False)

    logging.debug('disk_number: %s, drive_letter: %s', disk_number, drive_letter)
    logging.debug('no_admin: %s, no_crystaldiskinfo: %s', no_admin, no_crystaldiskinfo)

    if drive_letter and disk_number:
        du = psutil.disk_usage(drive_letter)
        logging.info('disk usage (%s|disk %s): %s%%', drive_letter, disk_number, du.percent)


def telemetry_thread(
    no_telemetry=constants.NO_TELEMETRY,
    no_admin=constants.NO_ADMIN,
    no_crystaldiskinfo=constants.NO_CRYSTALDISKINFO,
    all_drives=constants.ALL_DRIVES,
    poll=constants.POLL,
    smart_filepath=constants.SMART_FILEPATH,
    data_filepath=constants.DATA_FILEPATH,
    summary_filepath=constants.SUMMARY_FILEPATH,
    stop_event=constants.STOP_EVENT,
):
    # type: (bool, bool, bool, bool, float|int, str, str, str, threading.Event) -> Optional[threading.Thread]  # noqa: E501
    if no_telemetry:
        logging.warning('skipping telemetry!')
        return None

    logging.debug('checking admin access...')
    if system.admin_detect() != 0:
        if no_admin:
            logging.warning('running without admin privaleges, setting crystaldiskinfo low!')
            no_crystaldiskinfo = True
        else:
            raise RuntimeError('Must be run as administrator or sudo!')

    if not no_crystaldiskinfo:
        logging.debug('checking CrystalDiskInfo access...')
        if crystaldiskinfo_detect() != 0:
            raise RuntimeError('Cannot run CrystalDiskInfo!')

    t = threading.Thread(
        target=telemetry_async,
        kwargs=dict(
            no_telemetry=no_telemetry,
            no_admin=no_admin,
            stop_event=stop_event,
            smart_filepath=smart_filepath,
            data_filepath=data_filepath,
            summary_filepath=summary_filepath,
            poll=poll,
            no_crystaldiskinfo=no_crystaldiskinfo,
            all_drives=all_drives,
        )
    )
    t.start()
    return t
