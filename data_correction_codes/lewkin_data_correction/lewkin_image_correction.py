from pymongo import MongoClient

# MongoDB connection details
connection_string = "mongodb://root:iK&dsCaTio976fghI*(bgdskk)~@3.1.227.250:28018/tg_analytics?authSource=admin"
database_name = 'tg_analytics'
collection_name = 'crawler_sink_lewkin_south_korea'

# Connect to MongoDB
client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

# Find all documents with images that have URLs starting with "//"
documents_with_invalid_urls = collection.find({"images.url": {"$regex": "^//"}})

for document in documents_with_invalid_urls:
    doc_id = document["_id"]
    images = document.get("images", [])
    updated = False
    
    # Update URLs in the images array
    for i, image_obj in enumerate(images):
        if isinstance(image_obj, dict) and "url" in image_obj:
            url = image_obj["url"]
            if isinstance(url, str) and url.startswith("//"):
                new_url = url.replace("//cdn", "https://cdn", 1)  # Replace only the first occurrence
                images[i]["url"] = new_url
                updated = True
                print(f"Fixed URL: {url} -> {new_url}")
    
    # Update the document if any URLs were changed
    if updated:
        result = collection.update_one(
            {"_id": doc_id},
            {"$set": {"images": images}}
        )
        print(f"Updated document {doc_id}: {result.modified_count} document(s) modified")

# Close the connection
client.close()