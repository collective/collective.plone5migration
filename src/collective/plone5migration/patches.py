
from logging import getLogger


LOG = getLogger('root')

# patch for endless recursion

import plone.outputfilters

def my_apply_filters(filters, data):
    by_order = lambda x: x.order
    filters = sorted(filters, key=by_order)
    for filter in filters:
        if filter.is_enabled():
            try:
                res = filter(data)
            except Exception as e:
                LOG.warning('apply_filters() failed for filter {} ({})'.format(filter, e))
                res = None
            if res is not None:
                data = res
    return data

plone.outputfilters.apply_filters = my_apply_filters
