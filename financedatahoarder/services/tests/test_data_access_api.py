from functools import partial
from itertools import chain
import pkg_resources
import grequests
from financedatahoarder.services import data_access_api
from financedatahoarder.services.data_access_api import NonCachingAsyncRequestsClient
from nose.tools import eq_
from nose_parameterized import parameterized
from datetime import datetime, date, timedelta
from mock import patch
import pandas as pd
import logging


def _assert_equal_url_method_params_same(asyncresult_expected, asyncresult_actual):
    """Assert that url, method and params are equal"""
    eq_(asyncresult_expected.url, asyncresult_actual.url)
    eq_(asyncresult_expected.method, asyncresult_actual.method)
    eq_(asyncresult_expected.kwargs, asyncresult_actual.kwargs)


class DummyResponse(object):
    def __init__(self, content, status_code):
        self.content = content
        self.status_code = status_code
        self.url = 'dummy response url'

    def close(self): pass


def _yield_test_query_key_stats_correct_http_requests_data():
    for date_interval in [(datetime(2015, 1, 1, 12, 0), datetime(2015, 1, 2, 12, 0)),
                          (date(2015, 1, 1), date(2015, 1, 2))]:
        for urls, expected_requests in [
            (['http://url1.com', 'http://url2.com', 'http://url3.com'],
             [grequests.get('http://basehost.com/basepath/20150101/http://url1.com', params={}),
              grequests.get('http://basehost.com/basepath/20150101/http://url2.com', params={}),
              grequests.get('http://basehost.com/basepath/20150101/http://url3.com', params={}),
              grequests.get('http://basehost.com/basepath/20150102/http://url1.com', params={}),
              grequests.get('http://basehost.com/basepath/20150102/http://url2.com', params={}),
              grequests.get('http://basehost.com/basepath/20150102/http://url3.com', params={})]),

            (['http://url1.com', 'http://url2.com?id=2', 'http://url3.com?id=3'],
             [grequests.get('http://basehost.com/basepath/20150101/http://url1.com', params={}),
              grequests.get('http://basehost.com/basepath/20150101/http://url2.com', params={'id': '2'}),
              grequests.get('http://basehost.com/basepath/20150101/http://url3.com', params={'id': '3'}),
              grequests.get('http://basehost.com/basepath/20150102/http://url1.com', params={}),
              grequests.get('http://basehost.com/basepath/20150102/http://url2.com', params={'id': '2'}),
              grequests.get('http://basehost.com/basepath/20150102/http://url3.com', params={'id': '3'})])
        ]:
            yield date_interval, urls, 'http://basehost.com/basepath/', 4, expected_requests


@parameterized(_yield_test_query_key_stats_correct_http_requests_data())
def test_query_key_stats_correct_http_requests(date_interval, urls, base_replay_url, grequests_pool_size,
                                               expected_requests):
    client = NonCachingAsyncRequestsClient(base_replay_url, grequests_pool_size)
    with patch.object(data_access_api.grequests, 'imap') as grequests_imap, \
            patch.object(client, '_cdx_list') as cdx_list:
        cdx_list.return_value = {
            url: pd.Series(['http://basehost.com/basepath/{}/{}'.format(date.strftime('%Y%m%d'), url)
                            for date in pd.date_range(*date_interval)],
                           index=[date for date in pd.date_range(*date_interval)])
            for url in urls
        }

        _ = client.query_key_stats(date_interval, urls)

        cdx_list.assert_called_once_with(urls)

        eq_(len(grequests_imap.call_args_list), 1)
        actual_args, actual_kwargs = grequests_imap.call_args_list[0]
        eq_(actual_kwargs, {'size': grequests_pool_size})
        eq_(len(actual_args), 1)

        actual_requests = actual_args[0]
        eq_(len(actual_requests), len(expected_requests))
        for expected_request, actual_request in zip(expected_requests, actual_requests):
            _assert_equal_url_method_params_same(actual_request,
                                                 expected_request)


def _dummy_url_from_id(id):
    return 'http://dummyrequest.{id}.html'.format(id=id.replace('.html', ''))


def _dummy_url_from_filename(filename):
    return _dummy_url_from_id(filename.rpartition('_')[-1])


def _yield_test_query_key_stats_parsing_funds_http_200_data():
    url_to_expected_values = {
        'funds_snapshot_20150310_F0GBR04O2R.html': (pd.Timestamp('2015-03-09T00:00Z'), 6.65),
        'funds_snapshot_20150313_F0GBR04O2R.html': (pd.Timestamp('2015-03-13T00:00Z'), 6.74),
        'funds_snapshot_20150314_F0GBR04O2R.html': (pd.Timestamp('2015-03-13T00:00Z'), 6.74),
        'funds_snapshot_20150311_F0GBR04O2J.html': (pd.Timestamp('2015-03-10T00:00Z'), 2.51),
        'funds_snapshot_20150313_F0GBR04O2J.html': (pd.Timestamp('2015-03-12T00:00Z'), 2.51),
        'funds_snapshot_20150314_F0GBR04O2J.html': (pd.Timestamp('2015-03-13T00:00Z'), 2.51),
        'etf_snapshot_20150312_0P0000M7ZP.html': (pd.Timestamp('2015-03-12T00:00:00Z'), 116.18),
        'stock_20150320_knebv.html': (pd.Timestamp('2015-03-20T00:00Z'), 42.41)
    }

    def _return_test_data(ids_and_response_filenames, result_filenames):
        """Query urls represented by query_filenames, expect results corresponding to filenames result_filenames

        The query_filenames should match the expected query order (first time, then funds)
        """
        logger = logging.getLogger('_return_test_data')
        # Assert that test data is OK -- we should have equal number of responses
        expected_len = None
        for instrument_id, response_filenames in ids_and_response_filenames:
            if expected_len is None:
                expected_len = len(response_filenames)
                continue
            else:
                assert len(response_filenames) == expected_len

        # We should query one time instant, then the next and so forth
        response_filenames = [response_filenames for _, response_filenames in ids_and_response_filenames]
        response_filenames_flat_query_order = list(chain.from_iterable(zip(*response_filenames)))

        urls = [_dummy_url_from_id(instrument_id) for instrument_id, _ in ids_and_response_filenames]
        logger.debug(urls)
        return urls, response_filenames_flat_query_order, [{'value_date': url_to_expected_values[result_filename][0],
                                                            'value': url_to_expected_values[result_filename][1],
                                                            'instrument_url': _dummy_url_from_filename(result_filename)}
                                                           for result_filename in result_filenames]

    #
    # Funds
    #
    # one fund, in time order
    yield _return_test_data([('F0GBR04O2R', ['funds_snapshot_20150310_F0GBR04O2R.html',
                                             'funds_snapshot_20150314_F0GBR04O2R.html'])],
                            ['funds_snapshot_20150310_F0GBR04O2R.html', 'funds_snapshot_20150314_F0GBR04O2R.html'])

    # Same output even if the responses do not follow the logical chrono order
    yield _return_test_data([('F0GBR04O2R', ['funds_snapshot_20150314_F0GBR04O2R.html',
                                             'funds_snapshot_20150310_F0GBR04O2R.html'])],
                            ['funds_snapshot_20150310_F0GBR04O2R.html', 'funds_snapshot_20150314_F0GBR04O2R.html'])

    yield _return_test_data([('F0GBR04O2R', ['funds_snapshot_20150314_F0GBR04O2R.html',  # 2015-03-13
                                             'funds_snapshot_20150310_F0GBR04O2R.html',  # 2015-03-09
                                             'funds_snapshot_20150313_F0GBR04O2R.html',  # 2015-03-13
                                             ]),
                             ('F0GBR04O2J', ['funds_snapshot_20150311_F0GBR04O2J.html',  # 2015-03-10
                                             'funds_snapshot_20150313_F0GBR04O2J.html',  # 2015-03-12
                                             'funds_snapshot_20150314_F0GBR04O2J.html',  # 2015-03-13
                                             ]),
                             ],
                            [
                            # 2015-03-09
                            'funds_snapshot_20150310_F0GBR04O2R.html',
                            # 2015-03-10
                            'funds_snapshot_20150311_F0GBR04O2J.html',
                            # 2015-03-11
                            # 2015-03-12
                            'funds_snapshot_20150313_F0GBR04O2J.html',
                            # 2015-03-13, First R and then J due to query query order
                            'funds_snapshot_20150313_F0GBR04O2R.html',
                            'funds_snapshot_20150314_F0GBR04O2J.html',
                            ])
    # Otherwise same but different query order
    yield _return_test_data([('F0GBR04O2J', ['funds_snapshot_20150311_F0GBR04O2J.html',  # 2015-03-10
                                             'funds_snapshot_20150313_F0GBR04O2J.html',  # 2015-03-12
                                             'funds_snapshot_20150314_F0GBR04O2J.html',  # 2015-03-13
                                             ]),
                             ('F0GBR04O2R', ['funds_snapshot_20150314_F0GBR04O2R.html',  # 2015-03-13
                                             'funds_snapshot_20150310_F0GBR04O2R.html',  # 2015-03-09
                                             'funds_snapshot_20150313_F0GBR04O2R.html',  # 2015-03-13
                                             ]),
                             ],
                            [
                            # 2015-03-09
                            'funds_snapshot_20150310_F0GBR04O2R.html',
                            # 2015-03-10
                            'funds_snapshot_20150311_F0GBR04O2J.html',
                            # 2015-03-11
                            # 2015-03-12
                            'funds_snapshot_20150313_F0GBR04O2J.html',
                            # 2015-03-13, First J and then R due to query query order
                            'funds_snapshot_20150314_F0GBR04O2J.html',
                            'funds_snapshot_20150313_F0GBR04O2R.html',
                            ])

    # With some invalid responses
    yield _return_test_data([('F0GBR04O2J', ['invalid.html',  #
                                             'funds_snapshot_20150313_F0GBR04O2J.html',  # 2015-03-12
                                             'funds_snapshot_20150314_F0GBR04O2J.html',  # 2015-03-13
                                             ]),
                             ('F0GBR04O2R', ['funds_snapshot_20150314_F0GBR04O2R.html',  # 2015-03-13
                                             'funds_snapshot_20150310_F0GBR04O2R.html',  # 2015-03-09
                                             'funds_snapshot_20150313_F0GBR04O2R.html',  # 2015-03-13
                                             ]),
                             ],
                            [
                            # 2015-03-09
                            'funds_snapshot_20150310_F0GBR04O2R.html',
                            # 2015-03-10
                            #'funds_snapshot_20150311_F0GBR04O2J.html',
                            # 2015-03-11
                            # 2015-03-12
                            'funds_snapshot_20150313_F0GBR04O2J.html',
                            # 2015-03-13, First J and then R due to query query order
                            'funds_snapshot_20150314_F0GBR04O2J.html',
                            'funds_snapshot_20150313_F0GBR04O2R.html',
                            ])

    # ETF
    yield _return_test_data([('0P0000M7ZP', ['etf_snapshot_20150312_0P0000M7ZP.html']),
                             ],
                            [
                            'etf_snapshot_20150312_0P0000M7ZP.html'
                            ])


    # Stock
    yield _return_test_data([('knebv', ['stock_20150320_knebv.html']),
                             ],
                            [
                            'stock_20150320_knebv.html'
                            ])


@parameterized(_yield_test_query_key_stats_parsing_funds_http_200_data())
def test_query_key_stats_parsing_parsing_errors_but_all_http_200(urls, response_filenames, expected_key_stats):
    logger = logging.getLogger('test_query_key_stats_parsing_http_200')
    client = NonCachingAsyncRequestsClient('http://dummybaseurl.com', 4)
    with patch.object(data_access_api.grequests, 'imap') as grequests_imap, \
            patch.object(client, '_cdx_list'), \
            patch.object(data_access_api, 'prepare_replay_get'):
        responses = [pkg_resources.resource_stream(
            'financedatahoarder.services.tests', 'testdata/{}'.format(filename)).read() for filename in response_filenames]

        logger.debug(urls)
        num_requests = len(response_filenames)
        num_funds = len(urls)
        grequests_imap.return_value = map(partial(DummyResponse, status_code=200), responses)
        # We should have num_requests should be evenly divisible by num_funds
        assert num_requests / float(num_funds) == int(num_requests / num_funds)
        num_times = int(num_requests / num_funds)
        actual = client.query_key_stats((date(2015, 1, 1), date(2015, 1, 1) + timedelta(days=num_times - 1)),
                                 urls)

        # Basic assertion that input test data is correct
        actual_args, actual_kwargs = grequests_imap.call_args_list[0]
        eq_(actual_kwargs, {'size': 4})
        eq_(len(actual_args[0]), len(response_filenames))

        eq_(expected_key_stats, actual)
        print actual