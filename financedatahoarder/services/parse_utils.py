import logging

from financedatahoarder.scraper.scrapers.parsers.morningstar_overview import parse_overview_key_stats
import re
import requests
import pandas as pd
from scrapy import Selector


def parse_overview_key_stats_from_responses(responses):
    """Parse overview key stats from responses

    Closes the response as soon as the response is parsed.

    :param responses: list of responses
    :type responses: iterable[responses]
    :return: list of overview stats corresponding to responses
    :rtype: list[OverviewKeyStats]
    """

    def logger():
        return logging.getLogger('parse_overview_key_stats_from_responses')

    stats = []
    for response in responses:
        if response.status_code == requests.codes.ok:
            try:
                key_stats, = parse_overview_key_stats(Selector(text=response.content))
            except:
                logger().warning('Parse failed for {} -- ignoring entry'.format(response.url),
                                 exc_info=True)
                key_stats = {}
        else:
            logger().warning('HTTP status not 200 (was {response.status_code}) for {response.url} '
                             '-- ignoring entry'.format(response=response))
            key_stats = {}
        # Close the response to save number of connections
        # See http://stackoverflow.com/questions/23632794/in-requests-library-how-can-i-avoid-httpconnectionpool-is-full-discarding-con
        response.close()
        stats.append(key_stats)
    return stats


def parse_idx_list(response):
    """Parse pywb index page

    :return: Series indexed by query time. The value of the Series contains the replay URL corresponding to that time.
        Only the last successfull query (http 200) of each day is returned.
    :type: pd.Series
    """
    selector = Selector(text=response.content)
    rows = selector.xpath('//tr[position() > 1 and (not(td[2]/text()) or td[2]/text() = "200")]')
    dates = pd.Series(rows.xpath('.//script/text()').extract(), dtype=object)
    # date_js ~ document.write(ts_to_date("20150312190004", true))
    dates = dates.str.replace(re.escape('document.write(ts_to_date("'), '').str.replace(re.escape('", true))'), '')
    if not (dates.str.len() == 14).all():
        raise ValueError('Unexpected timestamp format')

    dates = pd.to_datetime(dates, format="%Y%m%d%H%M%S")
    links = rows.xpath('.//a/@href').extract()
    links = pd.Series(links, index=dates)
    # Pick last of each day
    links_daily = links.groupby(by=links.index.map(lambda dt: dt.date())).last()
    links_daily.index = pd.DatetimeIndex(links_daily.index)
    return links_daily