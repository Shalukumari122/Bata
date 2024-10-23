import pymysql
import scrapy
from scrapy.cmdline import execute
from bata.items import BataItem2


class BataSpider2Spider(scrapy.Spider):
    name = "bata_spider2"
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
            query = "select *from bata_table2 where status='pending'"
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
            subcat_of_cat_of_cat_name=row[7]
            subcat_of_cat_of_cat_link=row[8]
        # cat_name ='Women'
        # cat_link ='https://www.bata.com/in/women/'
        # subcat_name ='Shoes'
        # subcat_link ='https://www.bata.com/in/women/shoes/'
        # subcat_of_cat_name ='Casual Shoes'
        # subcat_of_cat_link = 'https://www.bata.com/in/women/shoes/casual-shoes/'
        # # subcat_of_cat_of_cat_name='Loafers'
        # # subcat_of_cat_of_cat_link='https://www.bata.com/in/women/shoes/casual-shoes/loafers/'
        # subcat_of_cat_of_cat_name=None
        # subcat_of_cat_of_cat_link=None
            if subcat_of_cat_of_cat_link:
                    yield scrapy.Request(url=subcat_of_cat_of_cat_link,meta={'cat_name':cat_name,'cat_link':cat_link,'subcat_name':subcat_name,'subcat_link':subcat_link,'subcat_of_cat_name':subcat_of_cat_name,'subcat_of_cat_link':subcat_of_cat_link,'subcat_of_cat_of_cat_name':subcat_of_cat_of_cat_name,'subcat_of_cat_of_cat_link':subcat_of_cat_of_cat_link},callback=self.parse3)
            else:
                    yield scrapy.Request(url=subcat_of_cat_link,meta={'cat_name':cat_name,'cat_link':cat_link,'subcat_name':subcat_name,'subcat_link':subcat_link,'subcat_of_cat_name':subcat_of_cat_name,'subcat_of_cat_link':subcat_of_cat_link,'subcat_of_cat_of_cat_name':subcat_of_cat_of_cat_name,'subcat_of_cat_of_cat_link':subcat_of_cat_of_cat_link},callback=self.parse2)

    def parse1(self,response):
        item = BataItem2()
        cat_name = response.meta.get('cat_name')
        cat_link = response.meta.get('cat_link')
        subcat_name = response.meta.get('subcat_name')
        subcat_link = response.meta.get('subcat_link')
        subcat_of_cat_name = response.meta.get('subcat_of_cat_name')
        subcat_of_cat_link = response.meta.get('subcat_of_cat_link')
        subcat_of_cat_of_cat_name = response.meta.get('subcat_of_cat_of_cat_name')
        subcat_of_cat_of_cat_link = response.meta.get('subcat_of_cat_of_cat_link')

        products=response.xpath('//div[@class="cc-row-product-grid product-grid cc-product-grid analytics-ProductList"]/div[contains(@class ,"cc-col-tile")]')

        for product in products:
            product_url=product.xpath('.//div[@class="image-container cc-image-container"]/a/@href').extract_first()
            product_url='https://bata.com'+product_url

            item['cat_name']=cat_name
            item['cat_link']=cat_link
            item['subcat_name']=subcat_name
            item['subcat_link']=subcat_link
            item['subcat_of_cat_name']=subcat_of_cat_name
            item['subcat_of_cat_link']=subcat_of_cat_link
            item['subcat_of_cat_of_cat_name'] = subcat_of_cat_of_cat_name
            item['subcat_of_cat_of_cat_link'] = subcat_of_cat_of_cat_link

            item['product_url']=product_url
            yield item

    def parse2(self, response):
        cat_name = response.meta.get('cat_name')
        cat_link = response.meta.get('cat_link')
        subcat_name = response.meta.get('subcat_name')
        subcat_link = response.meta.get('subcat_link')
        subcat_of_cat_name = response.meta.get('subcat_of_cat_name')
        subcat_of_cat_link = response.meta.get('subcat_of_cat_link')
        subcat_of_cat_of_cat_name = response.meta.get('subcat_of_cat_of_cat_name')
        subcat_of_cat_of_cat_link = response.meta.get('subcat_of_cat_of_cat_link')

        no_of_products=response.xpath('//div[@class="cc-container-plp-page-title-section"]/div/span/text()').extract_first().strip().replace('(', '').replace(')', '')

        last_page = (int(int(no_of_products) / 24) + 1) * 24

        last_page_url = subcat_of_cat_link + "?start=0&sz=" + str(last_page)



        yield scrapy.Request(url=last_page_url,
                             meta={'cat_name': cat_name, 'cat_link': cat_link, 'subcat_name': subcat_name,
                                   'subcat_link': subcat_link, 'subcat_of_cat_name': subcat_of_cat_name,
                                   'subcat_of_cat_link': subcat_of_cat_link,
                                   'subcat_of_cat_of_cat_name': subcat_of_cat_of_cat_name,
                                   'subcat_of_cat_of_cat_link': subcat_of_cat_of_cat_link}, callback=self.parse1,dont_filter=True)
    def parse3(self, response):
        cat_name = response.meta.get('cat_name')
        cat_link = response.meta.get('cat_link')
        subcat_name = response.meta.get('subcat_name')
        subcat_link = response.meta.get('subcat_link')
        subcat_of_cat_name = response.meta.get('subcat_of_cat_name')
        subcat_of_cat_link = response.meta.get('subcat_of_cat_link')
        subcat_of_cat_of_cat_name = response.meta.get('subcat_of_cat_of_cat_name')
        subcat_of_cat_of_cat_link = response.meta.get('subcat_of_cat_of_cat_link')

        no_of_products=response.xpath('//div[@class="cc-container-plp-page-title-section"]/div/span/text()').extract_first().strip().replace('(', '').replace(')', '')
        last_page=(int(int(no_of_products) / 24) + 1) * 24
        # ?start=0&sz=
        subcat_of_cat_link = response.meta.get('subcat_of_cat_link')
        last_page_url = subcat_of_cat_of_cat_link + "?start=0&sz=" + str(last_page)


        yield scrapy.Request(url=last_page_url,
                             meta={'cat_name': cat_name, 'cat_link': cat_link, 'subcat_name': subcat_name,
                                   'subcat_link': subcat_link, 'subcat_of_cat_name': subcat_of_cat_name,
                                   'subcat_of_cat_link': subcat_of_cat_link,
                                   'subcat_of_cat_of_cat_name': subcat_of_cat_of_cat_name,
                                   'subcat_of_cat_of_cat_link': subcat_of_cat_of_cat_link}, callback=self.parse1,dont_filter=True)



if __name__=='__main__':
    execute('scrapy crawl bata_spider2'.split())
