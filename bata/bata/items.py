# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BataItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = {}
class BataItem1(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = {}
class BataItem2(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = {}
class BataItem3(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    def __setitem__(self, key, value):
        self._values[key] = value
        self.fields[key] = {}



