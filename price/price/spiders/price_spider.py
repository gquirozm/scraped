# -*- coding: utf-8 -*-
import datetime
import urlparse
import socket
import scrapy

from scrapy.loader.processors import MapCompose, Join
from scrapy.loader import ItemLoader
from price.items import PriceItem
from scrapy.http import Request
from scrapy.linkextractors import LinkExtractor

class PriceSpider(scrapy.Spider):
	name = 'price_cl'
	allowed_domains = ["pricetravel.com.mx"]
	start_urls = ('http://www.pricetravel.com.mx/cancun/hoteles/3-estrellas-mas?true',
	#	'http://www.pricetravel.com.mx/playa-del-carmen/hoteles/3-estrellas-mas?true',
	#	'http://www.pricetravel.com.mx/acapulco/hoteles/3-estrellas-mas?true',
	#	'http://www.pricetravel.com.mx/puerto-vallarta/hoteles/3-estrellas-mas?true',
	#	'http://www.pricetravel.com.mx/ciudad-de-mexico/hoteles/3-estrellas-mas?true',

	)
	
	
	def parse(self, response):
		# Get the next index URLs and yield Requests
		next_selector = response.xpath('//*[@class="next"]//@href')
		for url in next_selector.extract():
		    yield Request(urlparse.urljoin("http://www.pricetravel.com.mx/", url))

        	# Get item URLs and yield Requests
		item_selector = response.xpath('//*[@class="itemInformation"]//@href')
		for url in item_selector.extract():
		    yield Request(urlparse.urljoin("http://www.pricetravel.com.mx/", url),
		                  callback=self.parse_item)
		 



	def parse_item(self, response):
		""" This function parses a property page.

		@url http://web:9312/properties/property_000000.html
		@returns items 1
		@scrapes title price description address image_urls
		@scrapes url project spider server date
		"""

		# Create the loader using the response
		l = ItemLoader(item=PriceItem(), response=response)
		item=PriceItem()

		# Load fields using XPath expressions
		item['city'] = ' '.join([x.encode('utf-8') for x in response.xpath('//*[@class="productName"]/h1/text()').extract()]).strip()
		item['name'] = ' '.join([x.encode('utf-8') for x in response.xpath('//*[@class="productName"]/h1/text()').extract()]).strip()
                    #MapCompose(lambda i: i.replace(',', ''), float),
                    #re='[.0-9]+')
		item['price'] = response.xpath('//*[contains(@class,"promotion-pay")]').re('[.0-9]+')
		item['address'] = ' '.join([x.encode('utf-8') for x in response.xpath('//*[@class="facts"]/p/text()').extract()]).strip()


		# Housekeeping fields
		item['link'] = response.url
		#item['project'] = self.settings.get('BOT_NAME')
		#item['spider'] = self.name
		item['source'] = 'price'
		item['date'] = datetime.datetime.now()

		#return l.load_item()
		return item
