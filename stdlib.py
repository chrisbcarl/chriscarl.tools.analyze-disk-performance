# stdlib
import os
import argparse


class NiceFormatter(
    argparse.ArgumentDefaultsHelpFormatter, argparse.RawTextHelpFormatter, argparse.RawDescriptionHelpFormatter
):
    pass


def abspath(*paths):
    return os.path.abspath(os.path.expanduser(os.path.join(*paths)))


def get_keys_from_dicts(*dicts):
    keys = []
    for dick in dicts:
        for key in dick.keys():
            if key not in keys:
                keys.append(key)
    return keys


def touch(filepath):
    with open(filepath, 'wb'):  # touch the file
        pass
