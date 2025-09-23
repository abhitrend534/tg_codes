import os
import json

countries = ['UK']

if os.path.exists('brand_pids.json'):
    with open('brand_pids.json', 'r') as json_file:
        temp = json.load(json_file)
else:
    temp = {}

for country in countries:
    temp[country] = {}
    country_path = f'{country}/Data'
    dates = os.listdir(country_path)
    for date in dates:
        gender_path = f'{country_path}/{date}/Json_data'
        if os.path.exists(gender_path):
            genders = os.listdir(gender_path)
            for gender in genders:
                files_path = f'{gender_path}/{gender}'
                files = os.listdir(files_path)
                for file in files:
                    file_path = f'{files_path}/{file}'
                    print(file_path)
                    with open(file_path, 'r') as json_file:
                        data = json.load(json_file)

                    pid = data['productId']
                    brand = data['brand']

                    if brand in temp[country].keys():
                        if pid not in temp[country][brand]:
                            temp[country][brand].append(pid)
                    else:
                        temp[country][brand] = [pid]
            
        with open('brand_pids.json', "w") as outfile:
            json.dump(temp, outfile, indent=4)