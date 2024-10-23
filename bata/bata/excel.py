import pandas as pd
import pymysql

con=pymysql.Connect(host='localhost',
                    user='root',
                    password='actowiz',
                    database='bata_db')
query="select *from data_table"
df=pd.read_sql(query,con)
# df.to_excel('bata_product_data.xlsx',index=False)
print(df.head())