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
    2025-08-11 - chrisbcarl - fixed a bug where disks werent getting initialized because numbers were tied to partitions
                              actually using read-disks.ps1
    2025-08-10 - chrisbcarl - it works and its so nice
                              fixed a bug where large files that dont match size were always loaded
    2025-08-09 - chrisbcarl - FULL reorganization and its actually 1000% better
                              log manipulation works a treat, still got a ways to go
                              full refactor complete, health and system are the only remaining ones to go
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
        - S.M.A.R.T. frequency
            >>> --poll 3

    - telemetry
        - loop
            >>> python main.py telemetry_loop --all-drives
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
                >>> python main.py write_burnin --size 4GB --log-every 16MB --chunk-size 64MB
        - fulpak (100% of the drive)
            - fulpak using a file that has a high throughput
                >>> python main.py write_fulpak --data-filepath I:/tmp
            - fulpak (leave the file behind) using a file of specific size
                >>> python main.py write_fulpak --data-filepath I:/tmp --size 4mb --no-delete

    - reading
        - read_seq
            - read_seq writes a file and reads from it ()
                >>> python main.py read_seq --size 4GB --chunk-size 16MB --log-every 128MB --no-telemetry
            - read_seq read an existing file w/o telemetry
                >>> python main.py read_seq `
                >>>     --data-filepath C/temp/chriscarl.tools.analyze-disk-performance/20250808-2249/data.dat `
                >>>     --no-telemetry
        - read_rand
            - read_rand writes a 4GB file and reads from it by randomly window hopping in 1MB chunks
                >>> python main.py read_rand --size 4GB --log-every 512MB --chunk-size 1MB --no-telemetry

    - flow
        - create + write_burnin + read_seq
            >>> python main.py flow --steps create write_burnin read_seq read_rand `
            >>>     --flow-iterations 3 `
            >>>     --data-filepath I:/tmp --size 4GB --value 69 --chunk-size 64MB --log-every 512MB
        - create + write_burnin + read_seq: only 1 minutes of this stuff
            >>> python main.py flow --steps create write_burnin read_seq read_rand `
            >>>     --flow-duration 60 `
            >>>     --data-filepath I:/tmp --size 4GB --value 69 --chunk-size 64MB --log-every 512MB

    - benchmarks
        - health: WARNING delete all partitions that arent in active use, run 3x fulpak write read
            >>> python main.py health

TODO:
    write_burnin needs
        --size as well as --total-size, where --size fills --total-size
'''
# stdlib
from __future__ import print_function, division
import os
import sys
import pprint
import string
import logging
import inspect
import argparse
import threading
from typing import Dict, List, Any  # noqa: F401

# 3rd party

# app imports
import constants as con
import smart
import system
import flow
import stdlib

SCRIPT_DIRPATH = os.path.abspath(os.path.dirname(__file__))

# i split them up so that during dynamic argparse parser creation they have separate groups
ARGUMENTS = {
    'data_filepath': dict(type=str, default=con.DATA_FILEPATH, help='file that stresses the disk', argtype='path'),
    'size': dict(type=str, default=con.SIZE, help='bytes, can use human friendly like 1024KB', argtype='str-int'),
    'value': dict(type=int, default=con.VALUE, help='default random, fill with constant value', min=0, max=255),
    'iterations': dict(type=int, default=con.ITERATIONS, help='repetitions, -1 for infinitely'),
    'duration': dict(type=float, default=con.DURATION, help='in seconds, -1 for infinitely'),
    'burn_in': dict(type=bool, help='default False, rewrite to the same place, not append and fill'),
    'steps': dict(type=str, nargs='+', choices=flow.FUNC_NAMES, required=True, help='run funcs in series'),
    'chunk_size':
        dict(type=str, default=con.CHUNK_SIZE, help='default -1, MUST evenly divide size, friendly', argtype='str-int'),
    'flow_iterations': dict(type=int, default=con.FLOW_ITERATIONS, help='repetitions, -1 for infinitely'),
    'flow_duration': dict(type=float, default=con.FLOW_DURATION, help='in seconds, -1 for infinitely'),
    'flow_no_delete_end': dict(type=bool, help='for convenience, --no-delete is set low for subflows, force high?'),
    'byte_array': dict(type=bytearray, default=None, help='WARNING: cannot be passed via cli'),
    'ignore_partitions':
        dict(type=str, nargs='*', default=con.IGNORE_PARTITIONS, help='if known partitions, ignore these'),
    'include_partitions':
        dict(
            type=str, nargs='*', default=[], choices=string.ascii_uppercase, help='override ignore, include only these'
        ),
    'disk_numbers': dict(type=int, nargs='*', default=con.DISK_NUMBERS, help='if known disk numbers, use these'),
    'disk_number_to_letter_dict': dict(type=str, help='pass as string, ex) {"1": "D:"}', argtype='json'),
    'log_every': dict(type=str, default='4GB', help='i/o log frequency, every X bytes', argtype='str-int'),
    'no_delete': dict(type=bool, help='default False, after operation, self-cleanup'),
    'no_cheat': dict(type=bool, help='default False, if True, dont apply this trick: if size > 1MB, simply repeat 1MB'),
    'stop_event':
        dict(type=threading.Event, default=con.STOP_EVENT, help='WARNING: cannot be passed via cli', argtype='lock'),
    # telemetry
    'all_drives': dict(type=bool, help='if enabled, it queries telemetry from all drives, rather than the one'),
    'poll': dict(type=float, default=con.POLL, help='telemetry poll poll'),
    'no_telemetry': dict(type=bool, help='skip telemetry entirely'),
    'no_admin': dict(type=bool, help='do what you can without admin'),
    'no_crystaldiskinfo': dict(type=bool, help='if disabled, you can run without admin!'),
    'smart_filepath':
        dict(type=str, default=con.SMART_FILEPATH, help='dump S.M.A.R.T. from CrystalDiskInfo.', argtype='path'),
    'summary_filepath': dict(type=str, default=con.SUMMARY_FILEPATH, help='afteraction summary', argtype='path'),
    'log_level': dict(type=str, default=con.LOG_LEVEL, choices=con.LOG_LEVELS, help='log level'),
    'log_format': dict(type=str, default=con.LOG_FORMAT, help='log format'),
}  # type: Dict[str, dict]
ARGUMENT_TYPES = {
    # special
    'str-int': [],
    'json': [],
    'path': [],
    'lock': [],
    # choices
    'choice': [],
    'choices': [],
    # numeric
    'pos': [],
    'range': [],
    # the rest
    'optional': [],
    'list': [],
    'singleton': [],
}  # type: Dict[str, List[str]]
for k, v in ARGUMENTS.items():
    argtype = v.get('argtype', '')
    if argtype:
        if argtype not in ARGUMENT_TYPES:
            raise NotImplementedError(f'argtype {argtype} not accounted for!')
        ARGUMENT_TYPES[argtype].append(k)
        continue
    if 'choices' in v:
        if 'nargs' not in v:
            ARGUMENT_TYPES['choice'].append(k)
            continue
        else:
            ARGUMENT_TYPES['choices'].append(k)
            continue

    if v['type'] in (int, float):
        if 'min' in v and 'max' in v:
            ARGUMENT_TYPES['range'].append(k)
            continue
        elif v.get('default', None) == -1:
            ARGUMENT_TYPES['pos'].append(k)
            continue

    if v.get('default', 'asdfasdfasdf') is None:
        ARGUMENT_TYPES['optional'].append(k)
    elif v.get('nargs'):
        ARGUMENT_TYPES['list'].append(k)
    else:
        ARGUMENT_TYPES['singleton'].append(k)
ARGUMENT_TO_TYPE = {val: key for key, lst in ARGUMENT_TYPES.items() for val in lst}
TELEMETRY_SIGNATURE = inspect.signature(smart.telemetry_thread)
TELEMETRY_PARAMETERS = TELEMETRY_SIGNATURE.parameters


def validate_kwargs(args):
    # type: (argparse.Namespace) -> None
    kwargs = vars(args)
    for argument, v in kwargs.items():
        if argument == 'func':
            assert kwargs[argument] in flow.FUNCS, 'this should never happen...'
            continue

        argument_type = ARGUMENT_TO_TYPE[argument]
        argument_kwargs = ARGUMENTS[argument]

        v = kwargs[argument]
        type_ = argument_kwargs.get('type', None)
        default = argument_kwargs.get('default', None)
        if argument_type == 'str-int':
            value = stdlib.validate_str_int(argument, v, default=default)
        elif argument_type == 'json':
            value = stdlib.validate_json(argument, v)
        elif argument_type == 'path':
            value = stdlib.validate_path(v)
        elif argument_type == 'lock':
            # contains dangerous objects like unpicklables and concurrent primitives
            value = default or type_()
        elif argument_type == 'choice':
            value = stdlib.validate_choice(argument, v, argument_kwargs['choices'])
        elif argument_type == 'choices':
            value = stdlib.validate_choices(argument, v, argument_kwargs['choices'])
        elif argument_type == 'pos':
            value = stdlib.validate_positive(argument, v, default=default)
        elif argument_type == 'range':
            value = stdlib.validate_range(argument, v, argument_kwargs['min'], argument_kwargs['max'], default=default)
        elif argument_type == 'optional':
            value = stdlib.validate_optional(argument, v, type_)
        elif argument_type == 'list':
            value = stdlib.validate_list(argument, v, type_)
        elif argument_type == 'singleton':
            value = stdlib.validate_singleton(argument, v, type_)
        else:
            raise NotImplementedError(f'validation of argument_type {argument_type!r} unaccounted for!')
        setattr(args, argument, value)


def post_process_kwargs(args):
    # type: (argparse.Namespace) -> None
    kwargs = vars(args)
    if 'flow_no_delete_end' in kwargs:
        if kwargs['flow_no_delete_end']:
            logging.debug('setting no_delete to False thanks to flow_no_delete_end True')
            kwargs['no_delete'] = False
        else:
            logging.debug('setting no_delete to True thanks to flow_no_delete_end False')
            kwargs['no_delete'] = True
        setattr(args, 'no_delete', kwargs['no_delete'])


def config(log_format=con.LOG_FORMAT, log_level=con.LOG_LEVEL, **kwargs):
    # type: (str, str, Any) -> None
    logging.basicConfig(format=log_format, level=log_level, stream=sys.stdout, force=True)


CONFIG_PARAMETERS = inspect.signature(config).parameters

# final developer sanity check
ALLOW_KIND = {
    inspect._POSITIONAL_OR_KEYWORD,  # type: ignore
}  # SKIP_KIND = (inspect._VAR_POSITIONAL, inspect._VAR_KEYWORD)
_ALL_FUNCS = flow.FUNCS + [config]
for _func in _ALL_FUNCS:
    _sig = inspect.signature(_func)
    for _k in _sig.parameters:
        if _k not in ARGUMENTS and stdlib.is_optional_with_default(_sig, _k):
            raise NotImplementedError(f'argument {_k!r} from {_func} not accounted for in ARGUMENTS!')


def add_argument_to_group_by_func(argument, group, parameters, prepend='--', include_underscore=False):
    argparse_kwargs = ARGUMENTS[argument]
    argparse_kwargs = {k: v for k, v in argparse_kwargs.items()}  # copy.deepcopy(argparse_kwargs)

    name_or_flags = []
    name_or_flag = f'{prepend}{argument}'
    if '_' in name_or_flag:
        name_or_flags.append(f'{name_or_flag.replace("_", "-")}')
        if include_underscore:
            name_or_flags.append(f'{name_or_flag}')  # dont include underscore --flag_s
    else:
        name_or_flags.append(f'{name_or_flag}')

    if 'default' in argparse_kwargs and argument in parameters:
        default = argparse_kwargs['default']
        func_default = parameters[argument].default
        if default != func_default:
            argparse_kwargs['default'] = func_default

    if argparse_kwargs['type'] is bool:
        del argparse_kwargs['type']
        argparse_kwargs['action'] = 'store_true'
    add_argument_kwargs = {k: v for k, v in argparse_kwargs.items() if k in stdlib.ARGPARSE_ADD_ARGUMENT_VAROPTIONAL}
    try:
        group.add_argument(*name_or_flags, **add_argument_kwargs)
    except argparse.ArgumentError as ae:
        aestr = str(ae)
        if 'conflicting option strings' in aestr:
            pass


def main():
    parser = argparse.ArgumentParser(prog=con.APP_NAME, description=__doc__, formatter_class=stdlib.NiceFormatter)
    operations = parser.add_subparsers(help='different operations we can do')

    for func in flow.FUNCS:
        op = operations.add_parser(
            func.__name__,
            help=func.__doc__.strip().splitlines()[1].strip(),  # just under Description:
            description=func.__doc__,
            formatter_class=stdlib.NiceFormatter,
        )
        op.set_defaults(func=func)

        if func is flow.flow:
            # forcibly graft on every single variant of parameter for every flow option
            for flow_func in flow.FUNCS:
                if flow_func is flow.flow:
                    continue
                group = op.add_argument_group(f'flow - {flow_func.__name__}')
                parameters = inspect.signature(flow_func).parameters
                for argument in ARGUMENTS:
                    if argument in parameters:
                        add_argument_to_group_by_func(
                            # prepend=f'--{flow_func.__name__}-'
                            argument,
                            group,
                            parameters,
                            prepend='--',
                            include_underscore=False
                        )

        # all functions are "normal" and follow the same rules for self-discovery
        operation_parameters = inspect.signature(func).parameters
        operation_group = op.add_argument_group('operation')
        telemetry_group = op.add_argument_group('telemetry')
        config_group = op.add_argument_group('config')

        for argument in ARGUMENTS:
            # in case we have to modify, and deepcopy doesnt work on lock objects
            exists_in_one_of_three_places = False
            if argument in operation_parameters:
                group = operation_group
                parameters = operation_parameters
                if argument in parameters:
                    exists_in_one_of_three_places = True
            elif argument in TELEMETRY_PARAMETERS:
                group = telemetry_group
                parameters = TELEMETRY_PARAMETERS
                if argument in parameters:
                    exists_in_one_of_three_places = True
            else:
                group = config_group
                parameters = CONFIG_PARAMETERS
                if argument in parameters:
                    exists_in_one_of_three_places = True
            if not exists_in_one_of_three_places:
                continue

            add_argument_to_group_by_func(argument, group, parameters, prepend='--', include_underscore=False)

    args = parser.parse_args()
    func = args.func

    validate_kwargs(args)
    kwargs = vars(args)
    config(**kwargs)  # update by side effect
    post_process_kwargs(args)  # update by side effect
    kwargs = vars(args)
    logging.debug('kwargs:\n%s', pprint.pformat(kwargs, indent=2))

    telemetry_thread_kwargs = {
        k: kwargs[k]
        for k in TELEMETRY_PARAMETERS if stdlib.is_optional_with_default(TELEMETRY_SIGNATURE, k)
    }
    logging.debug('telemetry_thread_kwargs:\n%s', pprint.pformat(telemetry_thread_kwargs, indent=2))

    # func_signature = inspect.signature(func)
    # func_parameters = func_signature.parameters
    # func_kwargs = {k: kwargs[k] for k in func_parameters if stdlib.is_optional_with_default(func_signature, k)}
    # logging.debug('func_kwargs: \n%s', pprint.pformat(func_kwargs, indent=2))

    stop_event = con.STOP_EVENT
    success = True
    thread = None
    try:
        if not any(
            [
                func is flow.flow and 'telemetry' in args.steps,
                func in {smart.telemetry, smart.telemetry_loop, system.create_partitions, system.delete_partitions},
            ]
        ):
            thread = smart.telemetry_thread(**telemetry_thread_kwargs)

        logging.info('starting %r', func.__name__)
        func(**kwargs)  # **func_kwargs
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
        logging.info('S.M.A.R.T. filepath: "%s"', kwargs.get('smart_filepath', 'n/a'))
        logging.info('Summary filepath: "%s"', kwargs.get('summary_filepath', 'n/a'))
        if success:
            logging.info('success!')
        else:
            logging.error('failure!')


if __name__ == '__main__':
    main()
