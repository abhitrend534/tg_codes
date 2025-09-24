from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_marknspencer_india'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

origins = ['  bangladesh', '  bangladesh/hong kong/egypt', '  bangladesh/sri lanka', '  bangladesh/sri lanka/united kingdom', '  cambodia', '  china', '  egypt', '  georgia', '  hong kong', '  india', '  italy', '  madagascar', '  mauritius', '  myanmar', '  serbia', '  sri lanka', '  tunisia', '  turkey', '  united kingdom', '  usa', '  vietnam']

for origin in origins:
    # Update origin field by stripping leading/trailing spaces and converting to lowercase
    result = collection.update_many(
        {"origin": origin},
        {"$set": {"origin": origin.split('/')[0].strip().lower()}}
    )

    print(f"Matched: {result.matched_count}, Modified: {result.modified_count}")

# Close the connection
client.close()