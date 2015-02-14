# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class TrailingReturns(scrapy.Item):
    # define the fields for your item here like:
    time_interval = scrapy.Field()
    returns_total = scrapy.Field()
    returns_class = scrapy.Field()
    returns_index = scrapy.Field()
    pass


class OverviewKeyStats(scrapy.Item):
    value = scrapy.Field()
    value_date = scrapy.Field()