# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class BookingItem(scrapy.Item):
    # define the fields for your item here like:
    city = scrapy.Field()
    name = scrapy.Field()
    price = scrapy.Field()
    address = scrapy.Field()
    link = scrapy.Field()
    latitud = scrapy.Field()
    longitud = scrapy.Field()

  # Housekeeping fields
    #url = scrapy.Field()
    #project = scrapy.Field()
    #spider = scrapy.Field()
    source = scrapy.Field()
    date = scrapy.Field()
    pass
