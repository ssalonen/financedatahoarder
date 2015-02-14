# -*- coding: utf-8 -*-
from financedatahoarder.scraper.scrapers.items import TrailingReturns
import scrapy
import pandas as pd


def parse_trailing_returns(selector):
    """Parses trailing returns table from tab=1 page of morningstar"""
    table = selector.css('table.returnsTrailingTable').extract()[0]
    df = pd.read_html(table, header=1, encoding='utf-8')[0]
    df.columns = 'time_interval', 'returns_total', 'returns_class', 'returns_index'
    return [TrailingReturns(df.to_dict('dict'))]