import datetime
import urlparse
import socket
import scrapy

from scrapy.loader.processors import MapCompose, Join
from scrapy.loader import ItemLoader
from booking.items import BookingItem
from scrapy.http import Request
from scrapy.linkextractors import LinkExtractor

class DespegarSpider(scrapy.Spider):
	name = 'booking_cl'
	allowed_domains = ["booking.com"]
#	start_urls = ('http://www.booking.com/city/mx/cancun.es.html',
#		'http://www.booking.com/city/mx/veracruz-mx.es.html',
#		'http://www.booking.com/city/mx/merida.es.html',
#		'http://www.booking.com/city/mx/mexico.es.html',
#		'http://www.booking.com/city/mx/acapulco-de-juarez.es.html',
#		'http://www.booking.com/city/mx/puerto-vallarta.es.html',
#		'http://www.booking.com/city/mx/playa-del-carmen.es.html',
#		'http://www.booking.com/region/mx/mayanriviere.es.html',
#	)

	start_urls = ('http://www.booking.com/searchresults.es.html?city=-1655011', 
               'http://www.booking.com/searchresults.es.html?city=-1707815',
               'http://www.booking.com/searchresults.es.html?city=-1683102',
               'http://www.booking.com/searchresults.es.html?city=-1658079',
               'http://www.booking.com/searchresults.es.html?city=-1649039',
               'http://www.booking.com/searchresults.es.html?city=-1690444',
               'http://www.booking.com/searchresults.es.html?city=-1689065',
               'http://www.booking.com/searchresults.es.html?region=2612',
       	)

	
	
	def parse(self, response):
		# Get the next index URLs and yield Requests
		next_selector = response.xpath('//*[contains(@class,"gotopage")]//@href')
		for url in next_selector.extract():
		    yield Request(url)

        	# Get item URLs and yield Requests
		item_selector = response.xpath('//*[contains(@class,"hotel_name_link url")]//@href')
		for url in item_selector.extract():
		    yield Request(urlparse.urljoin("http://www.booking.com/", url),
		                  callback=self.parse_item)
		 



	def parse_item(self, response):
		
		# Create the loader using the response
		l = ItemLoader(item=BookingItem(), response=response)
		item=BookingItem()
                item['city'] = ' '.join([x.encode('utf-8') for x in response.xpath('//*[@id="breadcrumb"]/div[4]/a/text()').extract()]).strip()
		item['name'] = ' '.join([x.encode('utf-8') for x in response.xpath('//*[@id="hp_hotel_name"]/text()').extract()]).strip()
		item['price'] = response.xpath('//*[@class="hf-infobox-price"]/p/text()').re('[.0-9]+')
                    #MapCompose(lambda i: i.replace(',', ''), float),
                    #re='[.0-9]+')
		lat = response.xpath('//*[@data-source="top_link"]//@data-bbox')[1].extract().split(',',2)[1]
  		lon = response.xpath('//*[@data-source="top_link"]//@data-bbox')[1].extract().split(',',3)[2]
		addr = ' '.join([x.encode('utf-8') for x in response.xpath('//*[@class="hp_address_subtitle jq_tooltip"]/text()').extract()]).strip()

		
		item['address'] = addr
		item['latitud'] = lat
		item['longitud'] = lon



		# Housekeeping fields
		item['link'] = response.url
		#item['project'] = self.settings.get('BOT_NAME')
		#item['spider'] = self.name
		item['source'] = 'booking'
		item['date'] = datetime.datetime.now()

		#return l.load_item()
		return item
