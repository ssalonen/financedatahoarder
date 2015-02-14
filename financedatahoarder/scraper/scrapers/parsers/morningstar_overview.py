# -*- coding: utf-8 -*-
import scrapy
import pandas as pd
from lxml import html
from lxml.html.clean import Cleaner
from pytz import UTC, timezone, FixedOffset
import logging
from financedatahoarder.scraper.scrapers.items import OverviewKeyStats

OSUUDEN_ARVO = 'Osuuden arvo'  # Fund
LOPETUSHINTA = 'Lopetushinta'  # ETF
MYYNTIKURSSI = 'Myyntikurssi'  # ETF

_HTML_CLEANER = Cleaner(allow_tags=[''], remove_unknown_tags=False)


def parse_overview_key_stats(selector):
    """Prase overview key stats from Morningstar.fi ETF, Fund or stocks page"""
    tables = selector.css('table.overviewKeyStatsTable').extract()
    if tables:
        table = tables[0]
        df = pd.read_html(table, encoding='utf-8')[0].transpose()
        df.columns = df.iloc[0]
        df = df.drop(0)
        for col, val in df.iteritems():
            if OSUUDEN_ARVO in col or MYYNTIKURSSI in col or LOPETUSHINTA in col:
                # Osuuden arvo<br>dd.mm.yyyy
                # Or
                # Myyntikurssi (dd.mm.yyyy)
                value_date = pd.to_datetime(col.replace(')', '')[-10:], dayfirst=True, utc=True)
                value = float(val.dropna().iloc[0].replace(',', '.').replace('EUR', '').strip())
                break
        else:
            raise RuntimeError('Could not find date')
        return [OverviewKeyStats(value=value, value_date=value_date)]
    else:
        return [parse_stock_price(selector)]


def parse_stock_price(selector):
    logger = logging.getLogger('parse_stock_price')
    # <span class="price" id="Col0Price">42,41</span>
    price_item = selector.css('span#Col0Price.price::text').extract()
    value = float(price_item[0].replace(',', '.'))
    # <p class="priceInformation" id="Col0PriceTime">Päivitetty 20.03.2015<br />18:29:38
    # <abbr title="TimeZone_EET">EET</abbr>
    datetime_text = selector.css('p#Col0PriceTime.priceInformation').extract()[0]
    datetime_text = html.fromstring(_HTML_CLEANER.clean_html(datetime_text)).text
    # datetime_text ~= u'Päivitetty 20.03.201518:29:38 EET | EUR \t\t ....
    date_text = datetime_text[10:21]
    time_text = datetime_text[21:29]
    tz_text = datetime_text[30:].partition('|')[0].strip()
    # UGLY: support for EEST (pytz does not support it), more proper support provided by this
    # https://github.com/mithro/python-datetime-tz/blob/master/datetime_tz/pytz_abbr.py
    if tz_text in ('EEST', 'EEDT'):
        tz = FixedOffset(3 * 60)
    else:
        tz = timezone(tz_text)
    value_date = pd.to_datetime(date_text + ' ' + time_text, dayfirst=True).tz_localize(tz)
    value_date = value_date.tz_convert(UTC)
    return OverviewKeyStats(value=value, value_date=value_date)

