import hashlib
import json
import os

import pymysql
import scrapy
from scrapy.cmdline import execute
from bata.items import BataItem3


class BataSpider3Spider(scrapy.Spider):
    name = "bata_spider3"
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
            # query = "select *from bata_table3 limit 1"

            query = "select * from link_table where status='pending'"
            # query = "select * from link_table limit 1"

            self.cur.execute(query)
            rows = self.cur.fetchall()
        except Exception as e:
            print(e)
        for row in rows:
            # id=row[0]
            product_url = row[1]
            cat_name = row[2]
            cat_link = row[3]
            subcat_name = row[4]
            subcat_link = row[5]
            subcat_of_cat_name = row[6]
            subcat_of_cat_link = row[7]
            subcat_of_cat_of_cat_name=row[8]
            subcat_of_cat_of_cat_link=row[9]


            yield scrapy.Request(url=product_url,
                                 meta={'cat_name':cat_name,'cat_link':cat_link,'subcat_name':subcat_name,'subcat_link':subcat_link,'subcat_of_cat_name':subcat_of_cat_name,'subcat_of_cat_link':subcat_of_cat_link,'subcat_of_cat_of_cat_name':subcat_of_cat_of_cat_name,'subcat_of_cat_of_cat_link':subcat_of_cat_of_cat_link,'product_url':product_url},callback=self.parse)

    def parse(self, response):
        item=BataItem3()
        # id=response.meta['id']
        cat_name = response.meta.get('cat_name')
        cat_link = response.meta.get('cat_link')
        subcat_name = response.meta.get('subcat_name')
        subcat_link = response.meta.get('subcat_link')
        subcat_of_cat_name = response.meta.get('subcat_of_cat_name')
        subcat_of_cat_link = response.meta.get('subcat_of_cat_link')
        subcat_of_cat_of_cat_name = response.meta.get('subcat_of_cat_of_cat_name')
        subcat_of_cat_of_cat_link = response.meta.get('subcat_of_cat_of_cat_link')
        product_url=response.meta.get('product_url')
        prod_id=product_url.split('?')
        prod_id=prod_id[-1]


        product_name=response.xpath('//h1[@class="product-name cc-pdp-product-name"]/text()').extract_first()
        product_price=response.xpath('//span/@content').extract_first()
        product_colors=response.xpath('//button[contains(@class,"color-attribute cc-color-attribute cc-imageAttribute")]/@data-value').extract()
        product_sizes=response.xpath('//li[contains(@class,"single-size-tile cc-single-size-tile")]/text()').extract()

        materials_type=response.xpath('//ul/li[@class="b-pdp__material-item cc-pdp-material-item"]/div/span/text()').extract()
        materials_info=response.xpath('//ul/li[@class="b-pdp__material-item cc-pdp-material-item"]/span[@class="b-pdp__material-info cc-pdp__material-info"]/span/text()').extract()
        artical_no=response.xpath('//span[@data-target="articleNo"]/text()').extract_first()
        if artical_no:
            artical_no=artical_no.strip()
        brand=response.xpath('//span[@data-target="brand"]/text()').extract_first()
        if brand:
            brand=brand.strip()
        manufacturer=response.xpath('//span[@data-target="manufacturerName"]/text()').extract_first()
        country_of_origin=response.xpath('//span[@data-target="madeIn"]/text()').extract_first()
        marketed_by=response.xpath('//span[@data-target="marketedBy"]/text()').extract_first()
        about_describtion={}
        about_describtion['Artical_no']=artical_no
        about_describtion['Brand']=brand
        about_describtion['Manufacturer']=manufacturer
        about_describtion['Country_Of_Origin']=country_of_origin
        about_describtion['Marketed_by']=marketed_by
        about_material={}
        about_material['Materials_Type']=materials_type
        about_material['Materials_Info']=materials_info

        images= response.xpath('//div[@class="pdp-images-carousel cc-pdp-images-carousel "]//div[@class="cc-container-dis-picture"]')
        img_list = []
        for image in images:
            i=image.xpath('./picture/img/@data-src').extract_first()
            img_list.append(i)


        item['image'] = ', '.join(img_list)
        item['cat_name']=cat_name
        item['cat_link'] = cat_link
        item['subcat_name'] = subcat_name
        item['subcat_link'] = subcat_link
        item['subcat_of_cat_name'] = subcat_of_cat_name
        item['subcat_of_cat_link'] = subcat_of_cat_link
        item['subcat_of_cat_of_cat_name'] = subcat_of_cat_of_cat_name
        item['subcat_of_cat_of_cat_link'] = subcat_of_cat_of_cat_link
        item['product_url']=product_url
        item['product_name'] = product_name
        item['product_price'] = product_price
        item['product_colors'] = json.dumps(product_colors)
        item['product_sizes'] = json.dumps(product_sizes)
        item['about_describtion']=json.dumps(about_describtion)
        item['about_material']=json.dumps(about_material)
        item['prod_id']=prod_id




        # hash_id = ""
        # if item['product_name']:
        #     if item['product_price']:
        #         hash_id = int(hashlib.md5(bytes(f"{item['product_name'] + item['product_price'] }","utf8")).hexdigest(), 16) % (10 ** 10)
        #         if item['product_colors']:
        #             hash_id = int(hashlib.md5(
        #                 bytes(f"{item['product_name'] + item['product_price'] + item['product_colors']}",
        #                       "utf8")).hexdigest(), 16) % (10 ** 10)
        #         else:
        #             hash_id = int(hashlib.md5(
        #                 bytes(f"{item['product_name'] + item['product_price']}",
        #                       "utf8")).hexdigest(), 16) % (10 ** 10)
        #
        #
        #     else:
        #         hash_id = int(hashlib.md5(
        #             bytes(f"{item['product_name']}",
        #                   "utf8")).hexdigest(), 16) % (10 ** 10)
        #
        #

        file_path = 'D:/bataPageSave/{}.html'.format(f"{prod_id}")
        if not os.path.exists(file_path):
            with open(file_path, 'wb') as html_file:
                html_file.write(response.body)

        yield item

if __name__=='__main__':
    execute('scrapy crawl bata_spider3'.split())
