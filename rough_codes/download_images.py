import requests
import os

# Add headers to mimic a real browser request
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
}

def download_image(brand, name, image):
    """Saves the downloaded image to a folder."""
    folder_path = os.path.join(os.getcwd(), brand)
    
    # Create the folder if it doesn't exist
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    file_path = os.path.join(folder_path, name)
    
    # Write the image content to a file
    with open(file_path, 'wb') as f:
        for chunk in image:
            f.write(chunk)

def get_images(brand, urls):
    """Downloads images from the given URLs and saves them using download_image."""
    for url in urls:
        name = url.split('?')[0].split('/')[-1]  # Extract the image name from the URL
        response = requests.get(url, headers=HEADERS, stream=True)

        if response.status_code == 200:
            download_image(brand, name, response.iter_content(1024))
        else:
            print(f"Failed to download {name} from {url}")

if __name__ == "__main__":
    brand_images = {
        'zara': [
            'https://static.zara.net/photos///2024/I/0/1/p/8068/130/051/2/w/2048/8068130051_1_1_1.jpg?ts=1716968111635',
            'https://static.zara.net/photos///2024/I/0/1/p/8068/130/051/2/w/2048/8068130051_2_1_1.jpg?ts=1716968110712',
            'https://static.zara.net/photos///2024/I/0/1/p/8068/130/051/2/w/2048/8068130051_6_1_1.jpg?ts=1717056154365'
        ],
        'bershka': [
            'https://static.bershka.net/4/photos2/2024/V/0/2/p/0479/665/711/0479665711_1_1_0.jpg?t=1706196641766',
            'https://static.bershka.net/4/photos2/2024/V/0/2/p/0479/665/711/0479665711_2_4_0.jpg?t=1706196641766',
            'https://static.bershka.net/4/photos2/2024/V/0/2/p/0479/665/711/0479665711_2_11_0.jpg?t=1706196641766'
        ],
        'mango': [
            'https://shop.mango.com/assets/rcs/pics/static/T6/fotos/S/67038269_05.jpg?ts=1709289398496',
            'https://shop.mango.com/assets/rcs/pics/static/T6/fotos/S/67038269_05_D1.jpg?ts=1709289398496',
            'https://shop.mango.com/assets/rcs/pics/static/T6/fotos/outfit/S/67038269_05-99999999_01.jpg?ts=1709289398496',
            'https://shop.mango.com/assets/rcs/pics/static/T6/fotos/S/67038269_05_R.jpg?ts=1709289398496'
        ],
        'gap': [
            'https://www.gap.com/webcontent/0057/349/996/cn57349996.jpg',
            'https://www.gap.com/webcontent/0057/350/036/cn57350036.jpg',
            'https://www.gap.com/webcontent/0056/938/193/cn56938193.jpg',
            'https://www.gap.com/webcontent/0057/350/073/cn57350073.jpg'
        ],
        'paige': [
            'https://cdn.shopify.com/s/files/1/0754/0411/6251/files/6526O43-1086_04.jpg?v=1729797544?width=100&height=100&quality=100',
            'https://cdn.shopify.com/s/files/1/0754/0411/6251/files/6526O43-1086_05.jpg?v=1729797684?width=100&height=100&quality=100',
            'https://cdn.shopify.com/s/files/1/0754/0411/6251/files/6526O43-1086_01.jpg?v=1729797153?width=100&height=100&quality=100'
        ]
    }
    
    for brand, urls in brand_images.items():
        get_images(brand, urls)
