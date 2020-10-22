# -*- coding: utf-8 -*-

import logging


def get_logger(filename, stdout=True):
    # Logging initialization (console + migration.log file)

    formatter = logging.Formatter("%(asctime)-8s %(message)s")
    log = logging.getLogger(filename)
    log.setLevel(logging.INFO)

    fh = logging.FileHandler(filename)
    fh.setFormatter(formatter)
    log.addHandler(fh)

    if stdout:
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        log.addHandler(ch)

    return log
