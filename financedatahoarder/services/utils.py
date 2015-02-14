import functools
import itertools
import operator
import numpy as np
import pandas as pd


def dataframe_from_list_of_dicts(dicts, index=None):
    """Convert list of dicts to DataFrame"""
    # XXX: FUGLY
    merged_data = {}
    for j in dicts:
        for k in j:
            merged_data[k] = []

    for dk, j in enumerate(dicts):
        if not hasattr(j, 'get'):
            raise ValueError('Wrong type, expecting dict, got: dicts[{}] = {} (of type {})'.format(dk, j, type(j)))
        for k in merged_data:
            merged_data[k].append(j.get(k, np.nan))

    return pd.DataFrame(merged_data, index=index)


def iter_sort_uniq(sequence, key):
    """Sort the sequence using `key` (function) and filter out consecutive unique items (as determined by key)"""
    return itertools.imap(next,
                          itertools.imap(
                              operator.itemgetter(1),
                              itertools.groupby(sorted(sequence, key=key), key=key))
    )