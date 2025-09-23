import pymongo
from datetime import datetime
from pymongo import InsertOne

# MongoDB connection details
connection1_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
connection2_string = "mongodb://localhost:27017/"
read_database_name = "tg_analytics"
target_database_name = "melody"

# Connect to MongoDB
client1 = pymongo.MongoClient(connection1_string)
client2 = pymongo.MongoClient(connection2_string)
source_db = client1[read_database_name]
target_db = client2[target_database_name]

collections = ['crawler_sink_h&m_india', 'crawler_sink_h&m_uk']

# Date filter
date_filter = {
    "date_of_scraping": {
        "$gte": datetime(2025, 7, 1, 0, 0, 0),
        "$lte": datetime(2025, 7, 7, 23, 59, 59)
    }
}

BATCH_SIZE = 1000

for collection_name in collections:
    source_collection = source_db[collection_name]
    target_collection = target_db[collection_name]

    print(f"🔄 Starting copy for collection: {collection_name}")
    total_copied = 0

    cursor = source_collection.find(date_filter, no_cursor_timeout=True).batch_size(BATCH_SIZE)

    batch_ops = []
    for doc in cursor:
        batch_ops.append(InsertOne(doc))

        if len(batch_ops) == BATCH_SIZE:
            target_collection.bulk_write(batch_ops, ordered=False)
            total_copied += len(batch_ops)
            print(f"⚡ Inserted {len(batch_ops)} documents... Total so far: {total_copied}")
            batch_ops = []

    if batch_ops:
        target_collection.bulk_write(batch_ops, ordered=False)
        total_copied += len(batch_ops)
        print(f"✅ Inserted final {len(batch_ops)} documents... Total copied: {total_copied}")

    cursor.close()
    print(f"✅ Finished: {collection_name} | Total: {total_copied}")

print("🎉 All collections copied successfully!")