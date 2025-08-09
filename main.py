# encoding: utf-8
'''
The MIT License (MIT)
Copyright © 2023 Chris Carl <chrisbcarl@outlook.com>
Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the “Software”), to
  deal in the Software without restriction, including without limitation the
  rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
  sell copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
  copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
  IN THE SOFTWARE.

Author:     Chris Carl <chrisbcarl@outlook.com>
Date:       2024-09-26
Modified:   2024-09-26

Modified:
    2025-08-09 - chrisbcarl - FULL reorganization and its actually 1000% better
                              log manipulation works a treat, still got a ways to go
    2025-08-07 - chrisbcarl - added create_partitions, delete_partitions
                              added health, now I just need to test
                              fine tuning after running it successfully, added defaults awareness
                              linting and re-org
    2025-08-06 - chrisbcarl - nearly full re-write, re-orged into app.py, will be further developed on
    2025-08-05 - chrisbcarl - moved files to the root of the perf filepath so the files
                                dont contend with each other for bandwidth or space
    2025-08-03 - chrisbcarl - moved functions around, perfected the read_bytearray_from_disk
                              cleaned up perf+fill+read minutia
    2025-08-02 - chrisbcarl - added perf+fill+read and smartmon
    2025-08-01 - chrisbcarl - changed to chriscarl.tools.analyze-disk-performance
                              created the 'health' module which is designed to rip through all disks
                              added crystaldiskinfo utilization, we'll see how it goes after testing.
    2024-09-28 - chrisbcarl - added "write" mode, much easier.
    2024-09-26 - chrisbcarl - complete rewrite with different modes this time
    2023-07-05 - chrisbcarl - init commit, behaves like sequential write/readback, thats it.

TODO:
    - have create be its own separate thing
        - have eveyrthing be its own thing.
        - define some common argument names, and within their OWN function, they can be --duration
            - during flow, they become --write_full-duration or --write_burn duration
    - separate write into write_full and write_burn, it'll just be easier that way.
    - deal with the situation where create_bytearray would result in a bytearray the size larger than the universe.
        better would be to make some object that when you ask for an index or the next byte,
        it GENRATES it, is writable, etc...
    - argparse flow it would be nice to have a dedicated "required" group but hey.

Examples:
    - general
        - to run without admin (and therefore no S.M.A.R.T.)
            >>> --no-admin
        - to run without any telemetry
            >>> --no-telemetry
        - adjust log level
            >>> --log-level DEBUG
        - adjust how often you see throughput and log messages during i/o
            >>> --log-every 16MB

    - telemetry
        - one drive (the drive of default data_filepath)
            >>> python main.py telemetry
        - all drives
            >>> python main.py telemetry --all-drives
        - one drive with a different partition
            >>> python main.py telemetry --data-filepath I:/
        - if not admin
            >>> python main.py telemetry --data-filepath I:/ --no-admin

    - create
        - creates a file that gets good write throughput
            >>> python main.py create --no-admin
        - creates a file of size KB
            >>> python main.py create --size 10KB --no-admin
        - if telemetry is not necessary
            >>> python main.py create --size 10KB --no-admin --no-telemetry

    - writing
        - burnin (one file only)
            - burnin using a file that has a high throughput in a different drive
                >>> python main.py write_burnin --data-filepath I:/tmp
            - burnin using a specific file size
                >>> python main.py write_burnin --size 1GB
        - fulpak (100% of the drive)
            - fulpak using a file that has a high throughput
                >>> python main.py write_fulpak --data-filepath I:/tmp
            - fulpak (leave the file behind) using a file of specific size
                >>> python main.py write_fulpak --data-filepath I:/tmp --size 4mb --no-delete

    - reading
        - read_seq
            - read_seq writes a file and reads from it ()
                >>> python main.py read_seq --size 1024 --no-admin
            - read_seq read an existing file w/o telemetry
                >>> python main.py read_seq `
                >>>     --data-filepath C/temp/chriscarl.tools.analyze-disk-performance/20250808-2249/data.dat `
                >>>     --no-telemetry
        - read_rand
            - read_rand writes a 4GB file and reads from it by randomly window hopping in 1MB chunks
                >>> python main.py read_rand --size 4GB --log-every 512MB --chunk_size 1MB --no-telemetry

TODO:
    write_burnin needs
        --size as well as --total-size, where --size fills --total-size


    - multi-drive
    ... # typical: take all drives, give them a partition, benchmark it, write-read, repeat 3x, S.M.A.R.T. monitoring
    >>> python main.py health

    ... # create a random file that writes very quickly
    >>> python main.py write --data-filepath C:/temp/tmp --iterations 1 --burn-in --search-optimal --skip-telemetry
    ... # create a file of 10mb
    >>> python main.py write --data-filepath C:/temp/tmp --size 10mb --iterations 1 --burn-in --skip-telemetry
    ... # read the file sequentially 4 times
    >>> python main.py read --data-filepath C:/temp/tmp --iterations 4 --skip-telemetry
    ... # read the file randomly forever, the randomness of the array jumps around in 256 windows, notify every 4 gbs
    >>> python main.py read --data-filepath C:/temp/tmp --random-read 1mb --skip-telemetry --log-unit GB --log-mod 4

    - flow: combine multiple stuff
    ... # read then write then telemetry without the asynchronous thread
    >>> python main.py flow --steps write read telemetry --burn-in --size 1kb --flow-iterations 1

    - flows
    - What most people think of as write-read benchmarking
        python main.py perf+fill+read --data-filepath Y:/temp
    - Find performance sweetspot and fill the disk at partition D:/
        python main.py perf+fill --data-filepath D:/temp
    - Evaluate overall health on all newly inserted disks
        python main.py health --ignore-partitions C --log-level DEBUG
    - Just launch a cyrstaldiskinfo monitor
        python main.py smartmon
'''
# stdlib
from __future__ import print_function, division
import os
import sys
import copy
import json
import pprint
import string
import logging
import inspect
import argparse

# 3rd party

# app imports
import constants
import smart
import system
import flow
from stdlib import NiceFormatter, size_unit_convert

SCRIPT_DIRPATH = os.path.abspath(os.path.dirname(__file__))

# i split them up so that during dynamic argparse parser creation they have separate groups
OPERATION_ARGUMENTS = {
    'data_filepath': dict(type=str, default=constants.DATA_FILEPATH, help='file that stresses the disk'),
    'size': dict(type=str, default=constants.SIZE, help='size in bytes, can use human friendly like 1024KB'),
    'value': dict(type=int, default=constants.VALUE, help='default random, fill with constant value'),
    'iterations': dict(type=int, default=constants.ITERATIONS, help='repetitions, -1 for infinitely'),
    'duration': dict(type=float, default=constants.DURATION, help='in seconds, -1 for infinitely'),
    'burn_in': dict(type=bool, help='default False, rewrite to the same place, not append and fill'),
    'steps': dict(type=str, nargs='+', choices=flow.FUNC_NAMES, required=True, help='run funcs in series'),
    'chunk_size':
        dict(type=str, default=constants.CHUNK_SIZE, help='default -1, MUST evenly divide size, randomly dart around'),
    'flow_iterations': dict(type=int, default=constants.FLOW_ITERATIONS, help='repetitions, -1 for infinitely'),
    'flow_duration': dict(type=float, default=constants.FLOW_DURATION, help='in seconds, -1 for infinitely'),
    'byte_array': dict(type=bytearray, nargs='+', default=bytearray(), help='BUG: used as part of dynamic programming'),
    'ignore_partitions':
        dict(type=str, nargs='*', default=constants.IGNORE_PARTITIONS, help='if known partitions, ignore these'),
    'include_partitions': dict(type=str, nargs='*', default=[], help='override ignore, include only these'),
    'disk_numbers': dict(type=int, nargs='*', default=constants.DISK_NUMBERS, help='if known disk numbers, use these'),
    'disk_number_to_letter_dict': dict(type=str, help='pass as string, ex) {"1": "D:"}'),
    'log_every': dict(type=str, default='4GB', help='i/o log frequency, every X bytes'),
    'no_delete': dict(type=bool, help='default False, after operation, self-cleanup'),
}
TELEMETRY_ARGUMENTS = {
    'all_drives': dict(type=bool, help='if enabled, it queries telemetry from all drives, rather than the one'),
    'poll': dict(type=float, default=constants.POLL, help='telemetry poll poll'),
    'no_telemetry': dict(type=bool, help='skip telemetry entirely'),
    'no_admin': dict(type=bool, help='do what you can without admin'),
    'no_crystaldiskinfo': dict(type=bool, help='if disabled, you can run without admin!'),
    'data_filepath': dict(type=str, default=constants.DATA_FILEPATH, help='file that stresses the disk'),
    'smart_filepath': dict(type=str, default=constants.SMART_FILEPATH, help='dump S.M.A.R.T. from CrystalDiskInfo.'),
    'summary_filepath': dict(type=str, default=constants.SUMMARY_FILEPATH, help='afteraction summary'),
    'log_level': dict(type=str, default=constants.LOG_LEVEL, choices=constants.LOG_LEVELS, help='log level'),
}
ARGUMENTS = {'operation': OPERATION_ARGUMENTS, 'telemetry': TELEMETRY_ARGUMENTS}


def validate_kwargs(
    args,
    func=flow,
    # members of lists
    log_level=constants.LOG_LEVEL,
    # numbers
    value=constants.VALUE,
    size=constants.SIZE,
    iterations=constants.ITERATIONS,
    flow_iterations=constants.FLOW_ITERATIONS,
    duration=constants.DURATION,
    flow_duration=constants.FLOW_DURATION,
    poll=constants.POLL,
    chunk_size=constants.CHUNK_SIZE,
    log_every=constants.LOG_EVERY,
    # files
    data_filepath=constants.DATA_FILEPATH,
    summary_filepath=constants.SUMMARY_FILEPATH,
    smart_filepath=constants.SMART_FILEPATH,
    # lists
    steps=None,
    byte_array=None,
    ignore_partitions=None,
    include_partitions=None,
    disk_numbers=None,
    # dicts
    disk_number_to_letter_dict=None,
    # bools
    no_optimizations=False,
    burn_in=constants.BURN_IN,
    no_crystaldiskinfo=constants.NO_CRYSTALDISKINFO,
    all_drives=constants.ALL_DRIVES,
    no_telemetry=constants.NO_TELEMETRY,
    no_admin=constants.NO_ADMIN,
    no_delete=constants.NO_DELETE,
):
    if func not in flow.FUNCS:
        raise KeyError(f'func {func!r} does not exist, use one of {flow.FUNCS}!')
    if log_level not in constants.LOG_LEVELS:
        raise KeyError(f'log_level {log_level!r} does not exist, choose one of {constants.LOG_LEVELS}!')

    # numbers
    if value != -1:
        if value < 0 and 255 < value:
            raise ValueError('value must be a value between [0,255] or -1')
    if isinstance(size, str):
        try:
            size = int(size)
        except ValueError:
            size = size_unit_convert(size)
        args.size = size
    if size != constants.SIZE and size <= 0:
        raise ValueError('size must be a postive int, are you nuts?')
    if iterations < constants.ITERATIONS:
        raise ValueError('iterations must be a postive num (or -1), are you nuts?')
    if flow_iterations != -1 and flow_iterations <= 0:
        raise ValueError('flow_iterations must be a postive num (or -1), are you nuts?')
    if duration != constants.DURATION and duration <= 0:
        raise ValueError('duration must be a postive num (or -1), are you nuts?')
    if flow_duration != constants.FLOW_DURATION and flow_duration <= 0:
        raise ValueError('flow_duration must be a postive num (or -1), are you nuts?')
    if poll < 0:
        raise ValueError('poll must be positive!')
    if isinstance(chunk_size, str):
        try:
            chunk_size = int(chunk_size)
        except ValueError:
            chunk_size = size_unit_convert(chunk_size)
        args.chunk_size = chunk_size
    if chunk_size != constants.CHUNK_SIZE and chunk_size <= 0:
        raise ValueError('chunk_size must be positive!')
    if isinstance(log_every, str):
        try:
            log_every = int(log_every)
        except ValueError:
            log_every = size_unit_convert(log_every)
        args.log_every = log_every
    if log_every != constants.LOG_EVERY and log_every <= 0:
        raise ValueError('log_every must be a postive int, are you nuts?')

    # files
    for filepath in [data_filepath, summary_filepath, smart_filepath]:
        if not os.path.isdir(os.path.dirname(filepath)):
            os.makedirs(os.path.dirname(filepath), exist_ok=True)

    # lists
    steps = steps or []
    if not isinstance(steps, list):
        raise TypeError(f'steps must be of type bool, provided {type(steps)}')
    assert all(
        step in flow.FUNC_NAMES for step in steps
    ), f'not all steps provided are real, only these are: {FUNC_NAMES}'
    byte_array = byte_array or bytearray()
    if not isinstance(byte_array, bytearray):
        raise TypeError(f'byte_array must be of type bytearray, provided {type(byte_array)}')
    if isinstance(ignore_partitions, list):
        for i, ignore_partition in enumerate(ignore_partitions):
            if ignore_partition not in string.ascii_uppercase:
                raise ValueError(f'ignore_partition {i + 1} {ignore_partition!r} not in expected possibilities!')
    if isinstance(include_partitions, list):
        for i, include_partition in enumerate(include_partitions):
            if include_partition not in string.ascii_uppercase:
                raise ValueError(f'include_partition {i + 1} {include_partition!r} not in expected possibilities!')
    if isinstance(disk_numbers, list):
        for i, disk_number in enumerate(disk_numbers):
            fail = False
            try:
                int(disk_number)
            except ValueError:
                fail = True
            if fail:
                raise ValueError(f'disk_number {i + 1} is not an int!')

    # dicts
    if disk_number_to_letter_dict:
        disk_number_to_letter_dict = json.loads(disk_number_to_letter_dict)
        args.disk_number_to_letter_dict = disk_number_to_letter_dict

    # bools
    if not isinstance(no_optimizations, bool):
        raise TypeError(f'no_optimizations must be of type bool, provided {type(no_optimizations)}')
    if not isinstance(burn_in, bool):
        raise TypeError(f'burn_in must be of type bool, provided {type(burn_in)}')
    if not isinstance(no_crystaldiskinfo, bool):
        raise TypeError(f'no_crystaldiskinfo must be of type bool, provided {type(no_crystaldiskinfo)}')
    if not isinstance(all_drives, bool):
        raise TypeError(f'all_drives must be of type bool, provided {type(all_drives)}')
    if not isinstance(no_telemetry, bool):
        raise TypeError(f'no_telemetry must be of type bool, provided {type(no_telemetry)}')
    if not isinstance(no_admin, bool):
        raise TypeError(f'no_admin must be of type bool, provided {type(no_admin)}')
    if not isinstance(no_delete, bool):
        raise TypeError(f'no_delete must be of type bool, provided {type(no_delete)}')


def main():
    parser = argparse.ArgumentParser(prog=constants.APP_NAME, description=__doc__, formatter_class=NiceFormatter)
    operations = parser.add_subparsers(help='different operations we can do')
    for func in flow.FUNCS:
        op = operations.add_parser(
            func.__name__,
            help=func.__doc__.strip().splitlines()[1].strip(),  # just under Description:
            description=func.__doc__,
            formatter_class=NiceFormatter,
        )
        op.set_defaults(func=func)

        if func is flow:
            # this one is especially meta because it "consumes" all kwargs, so we have to give it ALL arguments
            for group_name, argdict in ARGUMENTS.items():
                group = op.add_argument_group(group_name)
                for key, argparse_kwargs in argdict.items():
                    names = []
                    if '_' in key:
                        names.append(f'--{key.replace("_", "-")}')
                        names.append(f'--{key}')
                    else:
                        names.append(f'--{key}')
                    try:
                        if argparse_kwargs['type'] is bool:
                            group.add_argument(
                                *names,
                                action='store_true',
                                **{
                                    key: value
                                    for key, value in argparse_kwargs.items() if key != 'type'
                                }
                            )
                        else:
                            group.add_argument(*names, **argparse_kwargs)
                    except argparse.ArgumentError as ae:
                        aestr = str(ae)
                        if 'conflicting option strings' in aestr:
                            pass

        else:
            # all other functions are "normal" and follow other similar rules
            group = op.add_argument_group('operation')
            signature = inspect.signature(func)
            for key in signature.parameters:
                # print(func, key)
                argparse_kwargs = {k: v for k, v in OPERATION_ARGUMENTS.get(key, {}).items()}  # copy
                if 'default' in argparse_kwargs:
                    default = argparse_kwargs['default']
                    func_default = signature.parameters[key].default
                    if default != func_default:
                        argparse_kwargs['default'] = func_default
                if not argparse_kwargs:
                    continue
                names = []
                if '_' in key:
                    names.append(f'--{key.replace("_", "-")}')
                    names.append(f'--{key}')
                else:
                    names.append(f'--{key}')
                if argparse_kwargs['type'] is bool:
                    group.add_argument(
                        *names,
                        action='store_true',
                        **{
                            key: value
                            for key, value in argparse_kwargs.items() if key != 'type'
                        }
                    )
                else:
                    group.add_argument(*names, **argparse_kwargs)

            group = op.add_argument_group('telemetry')
            signature = inspect.signature(smart.telemetry_thread)
            for key, argparse_kwargs in TELEMETRY_ARGUMENTS.items():
                argparse_kwargs = copy.deepcopy(argparse_kwargs)
                # see if the function defines a special value instead, and use that.
                if 'default' in argparse_kwargs and key in signature.parameters:
                    default = argparse_kwargs['default']
                    func_default = signature.parameters[key].default
                    if default != func_default:
                        argparse_kwargs['default'] = func_default
                names = []
                if '_' in key:
                    names.append(f'--{key.replace("_", "-")}')
                    names.append(f'--{key}')
                else:
                    names.append(f'--{key}')
                try:
                    if argparse_kwargs['type'] is bool:
                        group.add_argument(
                            *names,
                            action='store_true',
                            **{
                                key: value
                                for key, value in argparse_kwargs.items() if key != 'type'
                            }
                        )
                    else:
                        group.add_argument(*names, **argparse_kwargs)
                except argparse.ArgumentError as ae:
                    aestr = str(ae)
                    if 'conflicting option strings' in aestr:
                        pass

            # for key in signature.parameters:
            #     # print(func, key)
            #     # if key in OPERATION_ARGUMENTS:
            #     #     continue
            #     if func == write:
            #         print('plx')
            #     if key not in TELEMETRY_ARGUMENTS:
            #         continue
            #     argparse_kwargs = {k: v for k, v in TELEMETRY_ARGUMENTS.get(key, {}).items()}  # copy
            #     if 'default' in argparse_kwargs:
            #         default = argparse_kwargs['default']
            #         func_default = signature.parameters[key].default
            #         if default != func_default:
            #             argparse_kwargs['default'] = func_default
            #     if not argparse_kwargs:
            #         continue
            #     names = []
            #     if '_' in key:
            #         names.append(f'--{key.replace("_", "-")}')
            #         names.append(f'--{key}')
            #     else:
            #         names.append(f'--{key}')
            #     try:
            #         if argparse_kwargs['type'] is bool:
            #             group.add_argument(
            #                 *names,
            #                 action='store_true',
            #                 **{
            #                     key: value
            #                     for key, value in argparse_kwargs.items() if key != 'type'
            #                 }
            #             )
            #         else:
            #             group.add_argument(*names, **argparse_kwargs)
            #     except argparse.ArgumentError as ae:
            #         aestr = str(ae)
            #         if 'conflicting option strings' in aestr:
            #             pass

    args = parser.parse_args()
    func = args.func

    kwargs = vars(args)
    validate_kwargs(args, **kwargs)  # update by side effect
    logging.basicConfig(
        format='%(asctime)s - %(levelname)10s - %(funcName)48s - %(message)s',
        level=args.log_level,
        stream=sys.stdout,
        force=True
    )
    kwargs = vars(args)
    logging.debug(pprint.pformat(kwargs, indent=2))

    telemetry_signature = inspect.signature(smart.telemetry_thread)
    telemetry_thread_kwargs = {k: kwargs[k] for k in telemetry_signature.parameters if k in TELEMETRY_ARGUMENTS}
    telemetry_thread_kwargs.update({k: kwargs[k] for k in telemetry_signature.parameters if k in OPERATION_ARGUMENTS})
    logging.debug('telemetry_thread_kwargs: \n%s', pprint.pformat(telemetry_thread_kwargs, indent=2))

    func_signature = inspect.signature(func)
    func_kwargs = {k: kwargs[k] for k in func_signature.parameters if k in OPERATION_ARGUMENTS}
    func_kwargs.update({k: kwargs[k] for k in func_signature.parameters if k in TELEMETRY_ARGUMENTS})
    logging.debug('func_kwargs: \n%s', pprint.pformat(func_kwargs, indent=2))

    stop_event = constants.STOP_EVENT
    success = True
    thread = None
    try:
        if not any(
            [
                func is flow and 'telemetry' in args.steps,
                func in {smart.telemetry, system.create_partitions, system.delete_partitions},
            ]
        ):
            thread = smart.telemetry_thread(**telemetry_thread_kwargs)

        logging.info('starting %r', func.__name__)
        func(**func_kwargs)
    except KeyboardInterrupt:
        logging.warning('ctrl + c detected!')
        logging.debug('ctrl + c detected!', exc_info=True)
    except Exception:
        logging.error('ERROR: exception encountered during execution!', exc_info=True)
        success = False
    finally:
        if stop_event:
            stop_event.set()
        if thread:
            thread.join()
        if success:
            logging.info('success!')
        else:
            logging.error('failure!')


if __name__ == '__main__':
    main()
