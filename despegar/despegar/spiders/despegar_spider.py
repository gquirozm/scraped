import datetime
import urlparse
import socket
import scrapy

from scrapy.loader.processors import MapCompose, Join
from scrapy.loader import ItemLoader
from despegar.items import DespegarItem
from scrapy.http import Request
from scrapy.linkextractors import LinkExtractor

class DespegarSpider(scrapy.Spider):
	name = 'despegar_cl'
	allowed_domains = ["despegar.com.mx"]
	start_urls = ('http://www.despegar.com.mx/hoteles/hl/7874/i1/hoteles-en-veracruz',
		'http://www.despegar.com.mx/hoteles/hl/1569/i1/hoteles-en-cancun',
	#	'http://www.despegar.com.mx/hoteles/hl/7874/i3/hoteles-en-veracruz'
	)
	
	
	def parse(self, response):
		# Get the next index URLs and yield Requests
		next_selector = response.xpath('//*[contains(@class,"next enabled")]//@href')
		for url in next_selector.extract():
		    yield Request(url)

        	# Get item URLs and yield Requests
		item_selector = response.xpath('//div[@class="hf-cluster-description-name  hf-raiting-on   "]/a/@href')
		for url in item_selector.extract():
		    yield Request(urlparse.urljoin("http://www.despegar.com.mx/", url),
		                  callback=self.parse_item)
		 



	def parse_item(self, response):
		
		# Create the loader using the response
		l = ItemLoader(item=DespegarItem(), response=response)
		item=DespegarItem()
                item['city'] = ' '.join([x.encode('utf-8') for x in response.xpath('//*[@class="ux-common-breadcrumbs"]/ul/li[4]/a/text()').extract()]).strip()
		item['name'] = ' '.join([x.encode('utf-8') for x in response.xpath('//*[@class="hf-header-name "]/h1/text()').extract()]).strip()
		item['price'] = response.xpath('//*[@class="hf-infobox-price"]/p/text()').re('[.0-9]+')
                    #MapCompose(lambda i: i.replace(',', ''), float),
                    #re='[.0-9]+')
		lat = response.xpath('//*[@class="hf-maps-container"]/div[2]//@style').re('[/][-.0-9]+')[2]
  		lon = response.xpath('//*[@class="hf-maps-container"]/div[2]//@style').re('[/][-.0-9]+')[3]
		addr = ' '.join([x.encode('utf-8') for x in response.xpath('//*[@class="hf-detail-box-module hf-detail-box-module-map hf-static-map-container"]/p[2]/text()').extract()]).strip()

		
		item['address'] = addr
		item['latitud'] = lat.replace('/','')
		item['longitud'] = lon.replace('/','')



		# Housekeeping fields
		item['link'] = response.url
		#item['project'] = self.settings.get('BOT_NAME')
		#item['spider'] = self.name
		item['source'] = 'despegar'
		item['date'] = datetime.datetime.now()

		#return l.load_item()
		return item
