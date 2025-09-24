from pymongo import MongoClient
import pandas as pd

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_lewkin_south_korea'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

unique_launch_prices = collection.distinct('launch_price')

temp = {}
for price in unique_launch_prices:
    unique_skus = collection.distinct('sku', {'launch_price': price})
    temp[price] = len(unique_skus)

# save temp as excel file
df = pd.DataFrame(list(temp.items()), columns=['launch_price', 'unique_sku_count'])
df.to_excel('unique_sku_counts_by_launch_price.xlsx', index=False)

# Close the connection
client.close()