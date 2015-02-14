import urlparse

from posixpath import join as urljoin
import grequests


def parse_params(qs):
    """Parse params from query string to requests compatible format"""
    params = urlparse.parse_qs(qs)
    # turn values to scalar
    params_scalar = {}
    for k, v in params.iteritems():
        params_scalar[k] = v[0]
    return params_scalar


def split_url_and_params(url):
    """Split url to base url and params"""
    base_url, _, qs = url.partition('?')
    params = parse_params(qs)
    return base_url, params


def grequest_get(url, session):
    """grequests.get wrapper for urls with params"""
    base_url, params = split_url_and_params(url)
    return grequests.get(base_url, params=params, session=session)


# def prepare_replay_get(replay_datetime, base_replay_url, url_to_replay):
#     if hasattr(replay_datetime, 'date'):
#         date = replay_datetime.date()
#     else:
#         date = replay_datetime
#     date_str = date.isoformat().replace('-', '')
#     full_url = urljoin(base_replay_url, "{date_str}1200/{url_to_replay}".format(**locals()))
#     # print full_url
#     req = grequest_get(full_url)
#     return req
#

def prepare_replay_get(replay_datetime, idx, session=None):
    if hasattr(replay_datetime, 'date'):
        date = replay_datetime.date()
    else:
        date = replay_datetime
    full_url = idx[replay_datetime]
    req = grequest_get(full_url, session=session)
    return req


def prepare_cdx_list_get(base_replay_url, url_to_replay, session=None):
    full_url = urljoin(base_replay_url, 'pywb-cdx', '*', url_to_replay)
    req = grequest_get(full_url, session=session)
    return req