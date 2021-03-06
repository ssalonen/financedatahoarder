from collections import OrderedDict
from cachecontrol import CacheControlAdapter
from cachecontrol.caches import FileCache
from cachecontrol.heuristics import ExpiresAfter
from requests import Session
import grequests
from financedatahoarder.services.http_utils import prepare_replay_get, prepare_cdx_list_get
from financedatahoarder.services.parse_utils import parse_overview_key_stats_from_responses, parse_idx_list
from financedatahoarder.services.utils import iter_sort_uniq
from financedatahoarder.scraper.scrapers.parsers.morningstar_overview import OverviewKeyStats
from toolz import groupby
import pandas as pd
import pytz
import logging
from itertools import chain
from redis_cache.rediscache import cache_it

class BaseClient(object):

    def query_key_stats(self, date_interval, urls):
        pass


class PyWbIndexBasedKeyStatsResolver(object):

    def __init__(self, url, cdx_list_func, prepare_replay_get_func, grequests_pool_size,
                 redis_cache):
        self.url = url
        self._cdx_list = cdx_list_func
        self._prepare_replay_get = prepare_replay_get_func
        self._grequests_pool_size = grequests_pool_size
        self._redis_cache = redis_cache
        self._logger = logging.getLogger('PyWbIndexBasedParser')

    def _from_cache(self, url, params, req):
        assert req.url == url
        assert req.kwargs['params'] == params
        # Hack to cache using url and params, but use req in query

        def _inner(req=req):
            @cache_it(cache=self._redis_cache)
            def query_from_cache(url, params):
                self._logger.debug("Will process %s", url)
                return self._process_noncached([req])[0]
            return query_from_cache
        return _inner()(url, params)

    def _process_noncached(self, reqs):
        responses = grequests.map(reqs, size=self._grequests_pool_size)
        key_stats = parse_overview_key_stats_from_responses(responses)
        return key_stats

    def _process_cached(self, reqs):
        key_stats = [self._from_cache(req.url, req.kwargs['params'], req) for req in reqs]
        return key_stats

    def parse(self, dates):
        url = self.url
        idx = self._cdx_list([url])

        prepared_requests = OrderedDict()
        for date in dates:
            try:
                prepared_get = self._prepare_replay_get(date, idx[url])
            except KeyError:
                self._logger.warning('Could not find replay of {url} for {date}'.format(**locals()))
            else:
                self._logger.debug('(date={}, url={}) -> {}'.format(date, url, prepared_get.url))

                prepared_requests[(date, url)] = prepared_get

        key_stats = self._process_cached(prepared_requests.values())

        return key_stats


class SeligsonCSVKeyStatsResolver(object):
    """
    TODO: move to parsers?
    https://github.com/nnarhinen/SeligsonRahastoarvot/blob/master/res/values/strings.xml
    """

    url_to_seligson_csv_url = {
        'http://www.morningstar.fi/fi/funds/snapshot/snapshot.aspx?id=F0GBR04O2R': 'http://www.seligson.fi/graafit/global-brands.csv',
        'http://www.morningstar.fi/fi/funds/snapshot/snapshot.aspx?id=F0GBR04UMF': 'http://www.seligson.fi/graafit/global-pharma.csv',
        'http://www.morningstar.fi/fi/funds/snapshot/snapshot.aspx?id=F0GBR04O2J': 'http://www.seligson.fi/graafit/rahamarkkina.csv'
    }

    @staticmethod
    def can_parse(cls, url):
        return url in cls.url_to_seligson_csv_url

    def __init__(self, url):
        """

        :param url:
        :return:
        :throw KeyError: on unsupported urls
        """
        self.url = self.url_to_seligson_csv_url[url]
        logger = logging.getLogger('SeligsonCSVKeyStatsResolver')
        logger.debug('Resolved {} to {}'.format(url, self.url))

    def parse(self, dates):
        logger = logging.getLogger('SeligsonCSVKeyStatsResolver')
        # FIXME: error handling
        logger.debug('Querying {}'.format(self.url))
        data = pd.read_csv(
                self.url, sep=';', names=['date', 'value'], dayfirst=True, parse_dates=['date'],
                index_col='date')
        try:
            data.index = data.index.tz_localize(dates.tz)
        except TypeError: pass

        try:
            data = data.loc[dates, 'value']
        except KeyError as e:
            # No match at all
            logger.error("Error: {}".format(e))
            return []

        logger.debug("Parsed: {}".format(data))

        return [{} if pd.isnull(value) else OverviewKeyStats(value=value, value_date=value_date)
            for value_date, value in data.iteritems()]


class DelegatingKeyStatsResolver(object):

    def __init__(self, cdx_list_func, prepare_replay_get_func, grequests_pool_size, redis_cache):
        self.cdx_list_func = cdx_list_func
        self.prepare_replay_get_func = prepare_replay_get_func
        self.grequests_pool_size = grequests_pool_size
        self.redis_cache = redis_cache

    def parse(self, url, dates):
        try:
            self.parser = SeligsonCSVKeyStatsResolver(url)
        except KeyError:
            self.parser = PyWbIndexBasedKeyStatsResolver(url, self.cdx_list_func, self.prepare_replay_get_func,
                                                         self.grequests_pool_size, self.redis_cache)

        return self.parser.parse(dates)



class DummyCache(object):
    connection = None


class NonCachingAsyncRequestsClient(BaseClient):

    def __init__(self, base_replay_url, grequests_pool_size, redis_cache=None, expire_after=0, expire_list_after=300):
        """
        :param base_replay_url: Base replay url to query
        :type base_replay_url: str
        :param grequests_pool_size: Pool size used with grequests queries
        :type grequests_pool_size: int
        :param expire_after: Cache expiration in seconds. None or zero to disable
        :type expire_after: int | None
        :param expire_list_after: CDX list cache expiration in seconds. None or zero to disable
        :type expire_list_after: int | None
        """
        redis_cache = DummyCache if redis_cache is None else redis_cache
        self.base_replay_url = base_replay_url
        self.grequests_pool_size = grequests_pool_size
        # We need to increase requests pool size since grequests makes many requests concurrently
        # See # See http://stackoverflow.com/questions/23632794/in-requests-library-how-can-i-avoid-httpconnectionpool-is-full-discarding-con
        requests_pool = max(10, 2 * grequests_pool_size)
        cache_adapter = CacheControlAdapter(cache=FileCache('.http_cache'),
                                            heuristic=ExpiresAfter(seconds=expire_after),
                                            pool_connections=requests_pool,
                                            pool_maxsize=requests_pool)
        self._session = Session()
        self._session.mount('http://', cache_adapter)
        self._list_session = Session()
        list_cache_adapter = CacheControlAdapter(cache=FileCache('.list_http_cache'),
                                                 heuristic=ExpiresAfter(seconds=expire_list_after))
        self._list_session.mount('http://', list_cache_adapter)
        self.redis_cache = redis_cache

    def _cdx_list(self, urls):
        """Return dict representing successful pywb recordings.

        :param urls: Urls to query recordings from pywb HTTP API
        :type urls: list[str]

        :return: dict for each url representing the successful recordings of that page. Dict value format is documented in
            :func:`parse_idx_list`.
        :rtype: dict[str, pd.Series]


        See also:

        :func:`parse_idx_list`
        """
        prepared_requests = []
        for url in urls:
            request = prepare_cdx_list_get(self.base_replay_url, url, session=self._list_session)
            prepared_requests.append(request)
        responses = grequests.map(prepared_requests, size=self.grequests_pool_size)
        idx = map(parse_idx_list, responses)
        idx = {url: idx for url, idx in zip(urls, idx)}
        return idx

    def prepare_replay_get(self, date, url_idx):
        return prepare_replay_get(date, url_idx, session=self._session)

    def query_key_stats(self, date_interval, urls):
        """

        :param date_interval:
        :param urls:
        :param base_replay_url:
        :param grequests_pool_size:
        :return:
        :rtype list:
        """
        if not urls:
            return []
        logger = logging.getLogger('query_key_stats')
        dates = pd.date_range(*date_interval)

        all_key_stats = []
        for url in urls:
            key_stats = DelegatingKeyStatsResolver(self._cdx_list,
                                                   self.prepare_replay_get, self.grequests_pool_size,
                                                   self.redis_cache).parse(url, dates)
            # Unwrap scrapy data types
            key_stats = map(dict, key_stats)
            for ks in key_stats:
                ks['instrument_url'] = url

            # Filter invalid
            key_stats = [key_stat for key_stat in key_stats if len(key_stat) > 1]

            # Day accuracy is enough
            for key_stat in key_stats:
                key_stat['value_date'] = pd.Timestamp(key_stat['value_date'].date(), tz=pytz.UTC)
            all_key_stats.extend(key_stats)

        sort_key_fun = lambda item: (item['value_date'], urls.index(item['instrument_url']))
        all_key_stats = list(iter_sort_uniq(all_key_stats, sort_key_fun))

        return all_key_stats