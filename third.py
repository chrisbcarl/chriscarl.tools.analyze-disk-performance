# stdlib
import os
import time
import logging
import threading  # noqa: F401
from typing import List  # noqa: F401

# third party
import psutil
import pandas as pd

# app
import constants

SCRIPT_DIRPATH = os.path.abspath(os.path.dirname(__file__))


def upsert_df_to_csv(df, filepath, index=False):
    # type: (pd.DataFrame, str, bool) -> List[str]
    dirpath = os.path.dirname(filepath)
    os.makedirs(dirpath, exist_ok=True)
    if os.path.isfile(filepath):
        old_df = pd.read_csv(filepath)
        new = pd.concat([old_df, df])
        new.to_csv(filepath, index=index)
        return new.columns.tolist()
    else:
        df.to_csv(filepath, index=index)
        return df.columns.tolist()


def disk_usage_monitor(stop_event=constants.STOP_EVENT, drive=constants.DRIVE):
    # type: (threading.Event, str) -> None
    prior_percent = None
    while not stop_event.is_set():
        du = psutil.disk_usage(drive)
        if str(du.percent) != str(prior_percent):
            logging.debug('disk usage: %s%%', du.percent)
            prior_percent = str(du.percent)
        for _ in range(100):
            time.sleep(1 / 100)
            if stop_event.is_set():
                break
