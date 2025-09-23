from pymongo import MongoClient
import pandas as pd

# MongoDB connection details
connection_string = "mongodb://tgReader:gfg57656h5Hjhgjlmncg8H7886745-ngRv@3.1.227.250:28018/tg_analytics"
database_name = 'footwear_analytics'
collection_name = 'crawler_sink_asics_india'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

unique_genders = collection.distinct('gender')

result = []
for gender in unique_genders:
    sizes = collection.distinct('size_name', {'gender': gender})
    for size in sizes:
        result.append({'gender': gender, 'size_name': size})

df = pd.DataFrame(result)
df.to_csv('asics_gender_size_name.csv', index=False)


# Close the connection
client.close()