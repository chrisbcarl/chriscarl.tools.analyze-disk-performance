# stdlib
from __future__ import print_function, division
import os
import time
import pprint
import logging
import inspect
from typing import Tuple, Dict, List, Optional  # noqa: F401

# 3rd party

# app imports
import constants
import input_output
import smart
import system

SCRIPT_DIRPATH = os.path.abspath(os.path.dirname(__file__))

# FUNC_NAMES = ['write', 'read', 'flow', 'telemetry', 'delete_partitions', 'create_partitions', 'health']
FUNCS = [
    input_output.create,
    input_output.write_burnin,
    input_output.write_fulpak,
    input_output.read_seq,
    input_output.read_rand,
    smart.telemetry,
    system.create_partitions,
    system.delete_partitions,
]
FUNC_NAMES = [func.__name__ for func in FUNCS]


def flow_run(steps=FUNC_NAMES, **kwargs):
    '''
    Description:
        run other functions in a long line
    '''
    gbls = globals()
    for s, step in enumerate(steps):
        func = gbls[step]
        logging.info('starting %s / %s - %r', s + 1, len(steps), func.__name__)

        signature = inspect.signature(func)
        subkwargs = {k: kwargs[k] for k in signature.parameters if k in kwargs}
        # otherwise its too much to print
        logging.debug(pprint.pformat({k: v for k, v in subkwargs.items() if k not in ['byte_array']}, indent=2))

        res = func(**subkwargs)
        if isinstance(res, bytearray):
            kwargs['byte_array'] = res
        elif func == system.delete_partitions:
            kwargs['disk_numbers'] = res
        elif func == system.create_partitions:
            kwargs['disk_number_to_letter_dict'] = res


def flow(steps=FUNC_NAMES, flow_iterations=constants.ITERATIONS, flow_duration=constants.DURATION, **kwargs):
    '''
    Description:
        run other functions serially
    '''
    iteration = 1
    if flow_iterations > -1:
        for iteration in range(1, flow_iterations + 1):
            logging.info('flow %d / %d', iteration, flow_iterations)
            flow_run(steps=steps, **kwargs)

    elif flow_duration > -1:
        start = time.time()
        elapsed = time.time() - start
        while elapsed < flow_duration:
            logging.info('flow until %s > %s, iteration %d', elapsed, flow_duration, iteration)
            flow_run(steps=steps, **kwargs)

            iteration += 1
            elapsed = time.time() - start

    else:
        while True:
            logging.info('flow infinitely, iteration %d', iteration)
            flow_run(steps=steps, **kwargs)

            iteration += 1
