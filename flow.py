# stdlib
from __future__ import print_function, division
import os
import pprint
import logging
import inspect
import threading  # noqa: F401
from typing import List, Any, Callable  # noqa: F401

# 3rd party

# app imports
import constants
import input_output
import smart
import system
import stdlib
import benchmarks

SCRIPT_DIRPATH = os.path.abspath(os.path.dirname(__file__))

# FUNC_NAMES = ['write', 'read', 'flow', 'telemetry', 'delete_partitions', 'create_partitions', 'health']
FUNCS = [
    input_output.create,
    input_output.write_burnin,
    input_output.write_fulpak,
    input_output.read_seq,
    input_output.read_rand,
    smart.telemetry,
    # TODO: test
    system.create_partitions,
    system.delete_partitions,
    benchmarks.health,
]  # type: List[Callable]
FUNC_MAP = {func.__name__: func for func in FUNCS}
FUNC_NAMES = [func.__name__ for func in FUNCS]


def flow_run(steps, stop_event=constants.STOP_EVENT, **kwargs):
    # type: (List[str], threading.Event, Any) -> None
    '''
    Description:
        Run other functions in a long line

    Arguments:
        steps: List[str]
            functions to run in a flow
        stop_event: threading.Event
            a way to short circuit exit if stop_event.is_set()
        **kwargs: varkwarguments

    Returns:
        None
    '''
    for s, step in enumerate(steps):
        if stop_event.is_set():
            break
        func = FUNC_MAP[step]
        logging.info('starting %s / %s - %r', s + 1, len(steps), func.__name__)

        signature = inspect.signature(func)
        # prepend = f'{step}_'
        # unmodified_kwargs = {k[len(prepend):]: v for k, v in kwargs.items() if k.startswith(prepend)}
        subkwargs = {k: kwargs[k] for k in signature.parameters if k in kwargs}  # unmodified_kwargs
        # otherwise its too much to print
        logging.debug(pprint.pformat({k: v for k, v in subkwargs.items() if k not in ['byte_array']}, indent=2))

        res = func(**subkwargs)
        if isinstance(res, bytearray):
            kwargs['byte_array'] = res
        elif isinstance(res, tuple) and len(res) == 3 and isinstance(res[2], bytearray):
            _, _, byte_array = res  # bytes_io, elapsed
            kwargs['byte_array'] = byte_array
        elif func == system.delete_partitions:
            kwargs['disk_numbers'] = res
        elif func == system.create_partitions:
            kwargs['disk_number_to_letter_dict'] = res


def flow(
    steps=FUNC_NAMES,
    flow_iterations=constants.ITERATIONS,
    flow_duration=constants.DURATION,
    flow_no_delete_end=constants.NO_DELETE,
    stop_event=constants.STOP_EVENT,
    **kwargs,
):
    # type: (List[str], int, float|int, bool, threading.Event, Any) -> Any|list|dict
    '''
    Description:
        Run other functions serially

    Arguments:
        steps: List[str]
            functions to run in a flow
        flow_iterations: int
            amount of iterations to keep runing for, leading to stop_event trigger
        flow_duration: float|int
            amount of time to keep runing for, leading to stop_event trigger
        stop_event: threading.Event
            a way to short circuit exit if stop_event.is_set()
        **kwargs: varkwarguments

    Returns:
        None
    '''
    try:
        return stdlib.loop_or_elapsed(
            flow_run,
            (steps, ),
            kwargs,
            description=f'flow-{"+".join(steps)}',
            result_behavior='singleton',
            iterations=flow_iterations,
            duration=flow_duration,
            stop_event=stop_event,
        )
    finally:
        if flow_no_delete_end:
            logging.warning('Finally deleting data_filepath at the end of the flow...')
            if os.path.isfile(kwargs['data_filepath']):
                os.remove(kwargs['data_filepath'])


FUNCS.append(flow)
FUNC_NAMES = [func.__name__ for func in FUNCS]
FUNC_MAP.update({func.__name__: func for func in FUNCS})
