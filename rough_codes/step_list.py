import os

main_folder = rf'C:\Users\abhis\OneDrive\Documents\Trendgully\scrape_master'
status_folders = os.listdir(main_folder)

for status_folder in status_folders:
    if status_folder in ['Complete', 'Working']:
        brands_folder_path = f'{main_folder}/{status_folder}'
        brands_folder = os.listdir(brands_folder_path)
        for brand_folder in brands_folder:
            program_folder_path = f'{brands_folder_path}/{brand_folder}'
            programs = os.listdir(program_folder_path)
            print(f'{brand_folder}: {programs}')