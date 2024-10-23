import scrapy
from scrapy.cmdline import execute
from bata.items import BataItem


class BataSpiderSpider(scrapy.Spider):
    name = "bata_spider"
    allowed_domains = ["bata.com"]
    start_urls = ["https://www.bata.com/in/"]
    # start_urls = ["https://www.bata.com/"]

    def parse(self, response):
        item=BataItem()
        cats=response.xpath('//nav/ul/span/li')[2:5]
        for cat in cats:
            cat_name=cat.xpath('./a/text()').extract_first()
            cat_link=cat.xpath('./a/@href').extract_first().strip()
            cat_link='https://www.bata.com'+cat_link
            subcats=cat.xpath('./ul//div[@class="cc-menu-2-col"]/li')
            for subcat in subcats:
                subcat_name=subcat.xpath('./a/text()').extract_first().strip()
                subcat_link=subcat.xpath('./a/@href').extract_first().strip()
                subcat_link='https://www.bata.com'+subcat_link
                subcat_of_cats = subcat.xpath('.//ul/li[@class="cc-menu-item cc-menu-item-3"]')
                for subcat_of_cat in subcat_of_cats:
                    subcat_of_cat_name=subcat_of_cat.xpath('./a/text()').extract_first().strip()
                    subcat_of_cat_link=subcat_of_cat.xpath('./a/@href').extract_first().strip()
                    subcat_of_cat_link='https://www.bata.com'+subcat_of_cat_link
                    item['cat_name']=cat_name
                    item['cat_link']=cat_link
                    item['subcat_name']=subcat_name
                    item['subcat_link']=subcat_link
                    item['subcat_of_cat_name']=subcat_of_cat_name
                    item['subcat_of_cat_link']=subcat_of_cat_link
                    yield item

if __name__=='__main__':
    execute('scrapy crawl bata_spider'.split())
