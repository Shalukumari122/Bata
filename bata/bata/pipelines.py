# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from bata.items import BataItem
from bata.items import BataItem1
from bata.items import BataItem2
from bata.items import BataItem3



class BataPipeline:
    def __init__(self):
        try:
            self.conn = pymysql.Connect(host='localhost',
                                        user='root',
                                        password='actowiz',
                                        database='bata_db')
            self.cur = self.conn.cursor()

        except Exception as e:
            print(e)
    def process_item(self, item, spider):
        if isinstance(item, BataItem):
            try:
                self.cur.execute(
                    "CREATE TABLE IF NOT EXISTS bata_table1(id INT AUTO_INCREMENT PRIMARY KEY)")
                self.cur.execute("SHOW COLUMNS FROM bata_table1")
                existing_columns = [column[0] for column in self.cur.fetchall()]
                item_columns = [column_name.replace(" ", "_") if " " in column_name else column_name for column_name in
                                item.keys()]
                for column_name in item_columns:
                    if column_name not in existing_columns:
                        column_name = column_name.lower()
                        try:
                            self.cur.execute(f"ALTER TABLE bata_table1 ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(e)
            except Exception as e:
                print(e)

            try:
                field_list = []
                value_list = []
                for field in item:
                    field_list.append(str(field))
                    value_list.append('%s')
                fields = ','.join(field_list)
                values = ", ".join(value_list)
                insert_query = f"insert ignore into bata_table1( " + fields + " ) values ( " + values + " )"
                self.cur.execute(insert_query, tuple(item.values()))
                self.conn.commit()
            except Exception as e:
                print(e)

        if isinstance(item, BataItem1):
            try:
                self.cur.execute(
                    "CREATE TABLE IF NOT EXISTS bata_table2(id INT AUTO_INCREMENT PRIMARY KEY)")
                self.cur.execute("SHOW COLUMNS FROM bata_table2")
                existing_columns = [column[0] for column in self.cur.fetchall()]
                item_columns = [column_name.replace(" ", "_") if " " in column_name else column_name for column_name in
                                item.keys()]
                for column_name in item_columns:
                    if column_name not in existing_columns:
                        column_name = column_name.lower()
                        try:
                            self.cur.execute(f"ALTER TABLE bata_table2 ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(e)
            except Exception as e:
                print(e)

            try:
                field_list = []
                value_list = []
                for field in item:
                    field_list.append(str(field))
                    value_list.append('%s')
                fields = ','.join(field_list)
                values = ", ".join(value_list)
                insert_query = f"insert ignore into bata_table2( " + fields + " ) values ( " + values + " )"
                self.cur.execute(insert_query, tuple(item.values()))
                self.conn.commit()
            except Exception as e:
                print(e)

            try:
                self.cur.execute(
                    f"UPDATE bata_table1 SET status='Done' WHERE subcat_of_cat_link='{item['subcat_of_cat_link']}'")
                self.conn.commit()
            except Exception as e:
                print(e)

        if isinstance(item, BataItem2):
            try:
                self.cur.execute("CREATE TABLE IF NOT EXISTS link_table(id INT AUTO_INCREMENT PRIMARY KEY,product_url varchar(255) unique)")
                self.cur.execute("SHOW COLUMNS FROM link_table")
                existing_columns = [column[0] for column in self.cur.fetchall()]
                item_columns = [column_name.replace(" ", "_") if " " in column_name else column_name for column_name in
                                item.keys()]
                for column_name in item_columns:
                    if column_name not in existing_columns:
                        column_name = column_name.lower()
                        try:
                            self.cur.execute(f"ALTER TABLE link_table ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(e)
            except Exception as e:
                print(e)

            try:
                field_list = []
                value_list = []
                for field in item:
                    field_list.append(str(field))
                    value_list.append('%s')
                fields = ','.join(field_list)
                values = ", ".join(value_list)
                insert_query = f"insert ignore into link_table( " + fields + " ) values ( " + values + " )"
                self.cur.execute(insert_query, tuple(item.values()))
                self.conn.commit()
            except Exception as e:
                print(e)

            if item['subcat_of_cat_of_cat_link']:
                try:
                    self.cur.execute(
                        f"UPDATE bata_table2 SET status='Done' WHERE subcat_of_cat_of_cat_link='{item['subcat_of_cat_of_cat_link']}'")
                    self.conn.commit()
                except Exception as e:
                    print(e)
            else:
                try:
                    self.cur.execute(
                        f"UPDATE bata_table2 SET status='Done' WHERE subcat_of_cat_link='{item['subcat_of_cat_link']}'")
                    self.conn.commit()
                except Exception as e:
                    print(e)


        if isinstance(item, BataItem3):
            try:
                self.cur.execute("CREATE TABLE IF NOT EXISTS data_table(id INT AUTO_INCREMENT PRIMARY KEY,product_url varchar(255) unique)")
                self.cur.execute("SHOW COLUMNS FROM data_table")
                existing_columns = [column[0] for column in self.cur.fetchall()]
                item_columns = [column_name.replace(" ", "_") if " " in column_name else column_name for column_name in
                                item.keys()]
                for column_name in item_columns:
                    if column_name not in existing_columns:
                        column_name = column_name.lower()
                        try:
                            self.cur.execute(f"ALTER TABLE data_table ADD COLUMN `{column_name}` LONGTEXT")
                            existing_columns.append(column_name)
                        except Exception as e:
                            print(e)
            except Exception as e:
                print(e)

            try:
                field_list = []
                value_list = []
                for field in item:
                    field_list.append(str(field))
                    value_list.append('%s')
                fields = ','.join(field_list)
                values = ", ".join(value_list)
                insert_query = f"insert ignore into data_table( " + fields + " ) values ( " + values + " )"
                self.cur.execute(insert_query, tuple(item.values()))
                self.conn.commit()
            except Exception as e:
                print(e)

            try:
                # self.cur.execute(
                #     f"UPDATE link_table SET status='Done' WHERE  cat_name='{item['cat_name']}' and subcat_name='{item['subcat_name']}' and subcat_of_cat_name='{item['subcat_of_cat_name']}' and product_url = '{item['product_url']}' ")
                self.cur.execute(
                    f"UPDATE link_table SET status='Done' WHERE  product_url = '{item['product_url']}' ")
                self.conn.commit()
            except Exception as e:
                print(e)

        return item
