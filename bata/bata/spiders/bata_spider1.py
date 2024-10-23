import pymysql
import scrapy
from scrapy.cmdline import execute
from bata.items import BataItem1


class BataSpider1Spider(scrapy.Spider):
    name = "bata_spider1"
    allowed_domains = ["bata.com"]
    # start_urls = ["https://bata.com"]
    def __init__(self):
        try:
            self.conn = pymysql.Connect(host='localhost',
                                        user='root',
                                        password='actowiz',
                                        database='bata_db')
            self.cur = self.conn.cursor()

        except Exception as e:
            print(e)


    def start_requests(self):
        try:
            query = "select *from bata_table1 where status='pending'"
            # query = "select *from bata_table1"

            self.cur.execute(query)
            rows = self.cur.fetchall()
        except Exception as e:
            print(e)
        for row in rows:
            cat_name = row[1]
            cat_link = row[2]
            subcat_name = row[3]
            subcat_link = row[4]
            subcat_of_cat_name = row[5]
            subcat_of_cat_link = row[6]
            # cat_name ='Women'
            # cat_link ='https://www.bata.com/in/women/'
            # subcat_name ='Collections'
            # subcat_link ='https://www.bata.com/in/women/collections/'
            # subcat_of_cat_name ='Online Exclusive'
            # subcat_of_cat_link = 'https://www.bata.com/in/women/collections/online-exclusive/'

            yield scrapy.Request(url=subcat_of_cat_link,
                                 meta={'cat_name': cat_name, 'cat_link': cat_link, 'subcat_name': subcat_name,
                                       'subcat_link': subcat_link, 'subcat_of_cat_name': subcat_of_cat_name,
                                       'subcat_of_cat_link': subcat_of_cat_link}, callback=self.parse)

    def parse(self, response):
        item=BataItem1()
        cat_name = response.meta.get('cat_name')
        cat_link = response.meta.get('cat_link')
        subcat_name = response.meta.get('subcat_name')
        subcat_link = response.meta.get('subcat_link')
        subcat_of_cat_name = response.meta.get('subcat_of_cat_name')
        subcat_of_cat_link = response.meta.get('subcat_of_cat_link')
        subcat_of_cat_of_cats=response.xpath('//div[@class="cc-filter-category cc-current-category"]/div[@class="cc-subCategories d-none"]/div[@class="cc-filter-category "]')
        if subcat_of_cat_of_cats:
            for subcat_of_cat_of_cat in subcat_of_cat_of_cats:
                subcat_of_cat_of_cat_name=subcat_of_cat_of_cat.xpath('./a/span[@class="sr-only selected-assistive-text"]/text()').extract_first().strip()
                subcat_of_cat_of_cat_link=subcat_of_cat_of_cat.xpath('./a/@href').extract_first()
                subcat_of_cat_of_cat_link='https://www.bata.com'+subcat_of_cat_of_cat_link
                item['cat_name'] = cat_name
                item['cat_link'] = cat_link
                item['subcat_name'] = subcat_name
                item['subcat_link'] = subcat_link
                item['subcat_of_cat_name'] = subcat_of_cat_name
                item['subcat_of_cat_link'] = subcat_of_cat_link
                item['subcat_of_cat_of_cat_name']=subcat_of_cat_of_cat_name
                item['subcat_of_cat_of_cat_link']=subcat_of_cat_of_cat_link
                yield item

        else:
            item['cat_name'] = cat_name
            item['cat_link'] = cat_link
            item['subcat_name'] = subcat_name
            item['subcat_link'] = subcat_link
            item['subcat_of_cat_name'] = subcat_of_cat_name
            item['subcat_of_cat_link'] = subcat_of_cat_link
            item['subcat_of_cat_of_cat_name'] = None
            item['subcat_of_cat_of_cat_link'] = None
            yield item





if __name__=='__main__':
    execute('scrapy crawl bata_spider1'.split())
