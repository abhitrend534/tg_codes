from pymongo import MongoClient
import pandas as pd

# MongoDB connection details
connection_string = "mongodb://localhost:27017/"
database_name = 'tg_analytics'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

brands_data = {}

countries = ['india', 'uk', 'usa']
for country in countries:
    brands_data[country] = []
    collection_name = f'crawler_sink_marknspencer_group_{country}'
    collection = db[collection_name]

    # Retrieve unique values for the key field
    unique_brands = collection.distinct('sub_brand')
    for brand in unique_brands:
        unique_product_ids = collection.distinct('product_id', {'sub_brand': brand})
        brands_data[country].append({'brand': brand, 'products': len(unique_product_ids)})

        print(f"Country: {country}, Brand: {brand}, Unique Products: {len(unique_product_ids)}")

# Create DataFrame from the brands data with MultiIndex columns
# Each country will have two sub-headers: 'brand' and 'products'
max_length = max(len(brands) for brands in brands_data.values())

data = {}
for country in countries:
    brands = brands_data.get(country, [])
    # Build parallel lists for brand names and product counts
    brand_list = [b['brand'] for b in brands] + [''] * (max_length - len(brands))
    prod_list = [b['products'] for b in brands] + [0] * (max_length - len(brands))
    data[(country, 'brand')] = brand_list
    data[(country, 'products')] = prod_list

# Create DataFrame and set MultiIndex columns
columns = pd.MultiIndex.from_tuples(list(data.keys()))
df = pd.DataFrame(data)
df.columns = columns

# Save to Excel file
excel_filename = 'unique_brands_by_country.xlsx'
# pandas does not support writing MultiIndex columns with index=False
# so write the dataframe including the index to preserve the MultiIndex headers
df.to_excel(excel_filename, index=True)
print(f"\nUnique brands saved to '{excel_filename}'")

# Close the connection
client.close()