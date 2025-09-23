import json

with open('unique_hnm_image_urls.json', 'r') as file:
    unique_image_urls = json.load(file)

print(f"Total unique image URLs: {len(unique_image_urls)}")

temp = {}

# Process each URL to remove query parameters
for url in unique_image_urls:
    if url.startswith('https://image.hm.com/assets'):
        updated_url = url.split('?')[0]
        temp[url] = updated_url
    elif url.startswith('https://lp2.hm.com/hmgoepprod'):
        image_code = url.split('.jpg')[0].split('%2F')[-1]
        updated_url = f'https://image.hm.com/assets/hm/{image_code[:2]}/{image_code[2:4]}/{image_code}.jpg'
        temp[url] = updated_url
    else:
        temp[url] = None

with open('unique_hnm_image_urls_updated.json', 'w') as file:
    json.dump(temp, file, indent=4)