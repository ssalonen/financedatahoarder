from collections import OrderedDict
from cachecontrol import CacheControlAdapter
from cachecontrol.caches import FileCache
from cachecontrol.heuristics import ExpiresAfter
from requests import Session
import grequests
from financedatahoarder.services.http_utils import prepare_replay_get, prepare_cdx_list_get
from financedatahoarder.services.parse_utils import parse_overview_key_stats_from_responses, parse_idx_list
from financedatahoarder.services.utils import iter_sort_uniq
import pandas as pd
import pytz
import logging


class BaseClient(object):

    def query_key_stats(self, date_interval, urls):
        pass


class NonCachingAsyncRequestsClient(BaseClient):

    def __init__(self, base_replay_url, grequests_pool_size, expire_after=0, expire_list_after=300):
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
        idx = self._cdx_list(urls)
        #logger.debug('IDX: {}'.format({k: v.tolist() for k, v in idx.iteritems()}))
        prepared_requests = OrderedDict()
        for date in dates:
            for url in urls:
                #prepared_get = prepare_replay_get(date, base_replay_url, url)
                try:
                    prepared_get = prepare_replay_get(date, idx[url], session=self._session)
                except KeyError:
                    logger.warning('Could not find replay of {url} for {date}'.format(**locals()))
                else:
                    logger.debug('(date={}, url={}) -> {}'.format(date, url, prepared_get.url))
                    prepared_requests[(date, url)] = prepared_get
        logger.debug('Mapping requests')
        responses = grequests.imap(prepared_requests.values(), size=self.grequests_pool_size)
        logger.debug('Mapping requests done')
        logger.debug('Parsing responses')
        key_stats = parse_overview_key_stats_from_responses(responses)
        logger.debug('Parsing responses done')
        # Unwrap scrapy data types
        key_stats = map(dict, key_stats)
        for key_stat, (date, url) in zip(key_stats, prepared_requests):
            key_stat['instrument_url'] = url
        # Filter invalid
        key_stats = [key_stat for key_stat in key_stats if len(key_stat) > 1]
        # Day accuracy is enough
        for key_stat in key_stats:
            key_stat['value_date'] = pd.Timestamp(key_stat['value_date'].date(), tz=pytz.UTC)

        sort_key_fun = lambda item: (item['value_date'], urls.index(item['instrument_url']))
        key_stats = list(iter_sort_uniq(key_stats, sort_key_fun))

        return key_stats