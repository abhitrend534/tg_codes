import json

with open('hnm_unique_color_images_1.json', 'r') as file:
    unique_color_images = json.load(file)


def updated_image_urls(images):
    for image in images:
        url = image['url']
        if url.startswith('https://image.hm.com/assets'):
            updated_url = url.split('?')[0]
        elif url.startswith('https://lp2.hm.com/hmgoepprod'):
            image_code = url.split('.jpg')[0].split('%2F')[-1]
            updated_url = f'https://image.hm.com/assets/hm/{image_code[:2]}/{image_code[2:4]}/{image_code}.jpg'

        image['url'] = updated_url

for color, images in unique_color_images.items():
    updated_image_urls(images)

with open('hnm_unique_color_images_updated_1.json', 'w') as file:
    json.dump(unique_color_images, file, indent=4)