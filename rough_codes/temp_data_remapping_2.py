from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://localhost:27017"
database_name = 'tg_analytics'

geographies = ['australia']

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]

for geography in geographies:
    collection_name = f'crawler_sink_zara_{geography}_kids'
    collection = db[collection_name]

    # Find documents and update product_id and brand_product_id
    for doc in collection.find({}, {"_id": 1, "size_name": 1, "product_ref_code": 1, "color_id": 1, "sku": 1}):
        size_name = doc.get("size_name")
        reference = doc.get("product_ref_code")
        cid = doc.get("color_id")
        sku = doc.get("sku")

        age_shortname = size_name.split(' (')[0]
        if 'year' in age_shortname:
            age_shortname = size_name.split('y')[0] + 'y'
        elif 'month' in age_shortname:
            age_shortname = size_name.split('m')[0] + 'm' 

        age_range = []
        if age_shortname == '1/2 y':
            age_shortname = '6 m'

        if '½' in age_shortname:
            age_shortname = age_shortname.replace('½', '.5')
            
        my = age_shortname.split(' ')[-1]
        numbers = age_shortname.split(' ')[0]

        if my == numbers:
            my = 'y'

        if '-' in numbers:
            n1 = int(numbers.split('-')[0])
            n2 = int(numbers.split('-')[1])
            for n in range(n1, n2+1):
                age_range.append(str(n) + my)
        elif '/' in numbers:
            n1 = int(numbers.split('/')[0])
            n2 = int(numbers.split('/')[1])
            for n in range(n1, n2+1):
                age_range.append(str(n) + my)
        else:
            age_range.append(numbers + my)

        pid = 'zar' + reference.split('-')[0]
        cid = cid.split('%')[1]
        sku = sku.split('%')[1]

        # Update the document
        collection.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "product_id": pid,
                    "age_range": age_range,
                    "color_id": f'{pid}%{cid}',
                    "sku": f'{pid}%{sku}'
                }
            }
        )

    print(f'{collection_name} processing complete.')

# Close the connection
client.close()