import os
from urllib import quote_plus
from StringIO import StringIO
import pkg_resources
import requests
from financedatahoarder.services.parse_utils import parse_idx_list
from nose_parameterized import parameterized
import pandas as pd
from pandas.util.testing import assert_series_equal


class DummyResponse(object):

    def __init__(self, content):
        self.content = content


def _test_parse_idx_list_data():
    # Each series entry should correspond to last HTTP 200 response of the day
    yield ('cdx_list.html', pd.Series([
                                    u'http://host.com/replay/20150307220004/http://tools.morningstar.fi/fi/stockreport/default.aspx?SecurityToken=0P0000A5Z8%5D3%5D0%5DE0WWE%24%24ALL',
                                    u'http://host.com/replay/20150308220008/http://tools.morningstar.fi/fi/stockreport/default.aspx?SecurityToken=0P0000A5Z8%5D3%5D0%5DE0WWE%24%24ALL',
                                    u'http://host.com/replay/20150309220005/http://tools.morningstar.fi/fi/stockreport/default.aspx?SecurityToken=0P0000A5Z8%5D3%5D0%5DE0WWE%24%24ALL',
                                    u'http://host.com/replay/20150310220004/http://tools.morningstar.fi/fi/stockreport/default.aspx?SecurityToken=0P0000A5Z8%5D3%5D0%5DE0WWE%24%24ALL',
                                    u'http://host.com/replay/20150311220004/http://tools.morningstar.fi/fi/stockreport/default.aspx?SecurityToken=0P0000A5Z8%5D3%5D0%5DE0WWE%24%24ALL',
                                    u'http://host.com/replay/20150312220005/http://tools.morningstar.fi/fi/stockreport/default.aspx?SecurityToken=0P0000A5Z8%5D3%5D0%5DE0WWE%24%24ALL',
                                    u'http://host.com/replay/20150313220005/http://tools.morningstar.fi/fi/stockreport/default.aspx?SecurityToken=0P0000A5Z8%5D3%5D0%5DE0WWE%24%24ALL',
                                    u'http://host.com/replay/20150314220005/http://tools.morningstar.fi/fi/stockreport/default.aspx?SecurityToken=0P0000A5Z8%5D3%5D0%5DE0WWE%24%24ALL',
                                    u'http://host.com/replay/20150315220005/http://tools.morningstar.fi/fi/stockreport/default.aspx?SecurityToken=0P0000A5Z8%5D3%5D0%5DE0WWE%24%24ALL',
                                    u'http://host.com/replay/20150316220008/http://tools.morningstar.fi/fi/stockreport/default.aspx?SecurityToken=0P0000A5Z8%5D3%5D0%5DE0WWE%24%24ALL',
                                    u'http://host.com/replay/20150317220004/http://tools.morningstar.fi/fi/stockreport/default.aspx?SecurityToken=0P0000A5Z8%5D3%5D0%5DE0WWE%24%24ALL',
                                    u'http://host.com/replay/20150318220008/http://tools.morningstar.fi/fi/stockreport/default.aspx?SecurityToken=0P0000A5Z8%5D3%5D0%5DE0WWE%24%24ALL',
                                    u'http://host.com/replay/20150319220008/http://tools.morningstar.fi/fi/stockreport/default.aspx?SecurityToken=0P0000A5Z8%5D3%5D0%5DE0WWE%24%24ALL',
                                    u'http://host.com/replay/20150320220008/http://tools.morningstar.fi/fi/stockreport/default.aspx?SecurityToken=0P0000A5Z8%5D3%5D0%5DE0WWE%24%24ALL',
                                    u'http://host.com/replay/20150321220006/http://tools.morningstar.fi/fi/stockreport/default.aspx?SecurityToken=0P0000A5Z8%5D3%5D0%5DE0WWE%24%24ALL',
                                    u'http://host.com/replay/20150322130004/http://tools.morningstar.fi/fi/stockreport/default.aspx?SecurityToken=0P0000A5Z8%5D3%5D0%5DE0WWE%24%24ALL'],
                                index=[pd.Timestamp('2015-03-07 00:00:00', offset='D'),
                                       pd.Timestamp('2015-03-08 00:00:00', offset='D'),
                                       pd.Timestamp('2015-03-09 00:00:00', offset='D'),
                                       pd.Timestamp('2015-03-10 00:00:00', offset='D'),
                                       pd.Timestamp('2015-03-11 00:00:00', offset='D'),
                                       pd.Timestamp('2015-03-12 00:00:00', offset='D'),
                                       pd.Timestamp('2015-03-13 00:00:00', offset='D'),
                                       pd.Timestamp('2015-03-14 00:00:00', offset='D'),
                                       pd.Timestamp('2015-03-15 00:00:00', offset='D'),
                                       pd.Timestamp('2015-03-16 00:00:00', offset='D'),
                                       pd.Timestamp('2015-03-17 00:00:00', offset='D'),
                                       pd.Timestamp('2015-03-18 00:00:00', offset='D'),
                                       pd.Timestamp('2015-03-19 00:00:00', offset='D'),
                                       pd.Timestamp('2015-03-20 00:00:00', offset='D'),
                                       pd.Timestamp('2015-03-21 00:00:00', offset='D'),
                                       pd.Timestamp('2015-03-22 00:00:00', offset='D')]))
    yield ('cdx_list_no_status.html', pd.Series([
                                    u'http://host.com/replay/20150403210014/http://www.morningstar.fi/fi/funds/snapshot/snapshot.aspx?id=F0GBR04OGI',
                                    u'http://host.com/replay/20150404210015/http://www.morningstar.fi/fi/funds/snapshot/snapshot.aspx?id=F0GBR04OGI',
                                    u'http://host.com/replay/20150405180012/http://www.morningstar.fi/fi/funds/snapshot/snapshot.aspx?id=F0GBR04OGI'],
                                index=[pd.Timestamp('2015-04-03 00:00:00', offset='D'),
                                       pd.Timestamp('2015-04-04 00:00:00', offset='D'),
                                       pd.Timestamp('2015-04-05 00:00:00', offset='D')]))
    # urls = ('http://www.morningstar.fi/fi/funds/snapshot/snapshot.aspx?id=F0GBR04UMF',
    #         'http://www.morningstar.fi/fi/funds/snapshot/snapshot.aspx?id=F0GBR04O2R',
    #         'http://www.morningstar.fi/fi/funds/snapshot/snapshot.aspx?id=F0GBR04O2J',
    #         'http://www.morningstar.fi/fi/funds/snapshot/snapshot.aspx?id=F0GBR04OGI',
    #         'http://www.morningstar.fi/fi/funds/snapshot/snapshot.aspx?id=0P0000GGNP',
    #         'http://www.morningstar.fi/fi/funds/snapshot/snapshot.aspx?id=F00000UF2B',
    #         'http://www.morningstar.fi/fi/etf/snapshot/snapshot.aspx?id=0P0000M7ZP',
    #         'http://www.morningstar.fi/fi/etf/snapshot/snapshot.aspx?id=0P0000RTOH',
    #         'http://www.morningstar.fi/fi/etf/snapshot/snapshot.aspx?id=0P0000MEI0',
    #         'http://tools.morningstar.fi/fi/stockreport/default.aspx?SecurityToken=0P0000A5Z8]3]0]E0WWE$$ALL')
    # for url in urls:
    #     resp = 'http://host.com/archive.org/replay/pywb-cdx/*/{url}'.format(url=url)
    #     yield (resp, pd.Series())


@parameterized(_test_parse_idx_list_data)
def test_parse_idx_list(response_filename, expected_idx):
    if not response_filename.startswith('http://'):
        response = DummyResponse(pkg_resources.resource_stream(
            'financedatahoarder.services.tests', 'testdata/{}'.format(response_filename)).read())
    else:
        response_fname = quote_plus(response_filename)
        response_path = pkg_resources.resource_filename(
                'financedatahoarder.services.tests', 'testdata/{}'.format(response_fname))
        if os.path.exists(response_path):
            response = DummyResponse(open(response_path).read())
        else:
            response = DummyResponse(requests.get(response_filename).content)
            with open(response_path, 'w') as f:
                print 'storing response'
                f.write(response.content)
    actual_idx = parse_idx_list(response)
    s = StringIO()
    actual_idx.to_csv(s, sep=';')
    print 'actual:\n', s.getvalue()
    print '/actual'
    assert_series_equal(expected_idx, actual_idx)
