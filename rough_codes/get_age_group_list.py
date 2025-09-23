import json

def get_my(age_range):
    return age_range[0][-1]

def get_age_group(age_range):
    my = get_my(age_range)
    age_groups = []
    if my == 'm':
        for age in age_range:
            if age in ['7m', '8m', '9m', '10m', '11m', '12m', '13m', '14m', '15m', '16m', '17m', '18m', '19m', '20m', '21m', '22m', '23m', '24m']:
                if 'baby' not in age_groups:
                    age_groups.append('baby')
            elif age in ['0m', '1m', '2m', '3m', '4m', '5m', '6m']:
                if 'new_born' not in age_groups:
                    age_groups.append('new_born')
            else:
                if 'baby' not in age_groups:
                    age_groups.append('baby')
    elif my == 'y':
        if age_range == ['18y']:
            age_groups.append('adult')
        for age in age_range:
            if age in ['13y', '14y', '15y', '16y', '17y']:
                if 'teen' not in age_groups:
                    age_groups.append('teen')
            elif age in ['8y', '9y', '10y', '11y', '12y']:
                if 'senior' not in age_groups:
                    age_groups.append('senior')
            elif age in ['0y', '0.75y', '1y', '1.5y', '2y', '3y', '4y', '5y', '6y', '7y']:
                if 'junior' not in age_groups:
                    age_groups.append('junior')
    return age_groups

read_file = 'unique_age_ranges.json'

with open(read_file, 'r') as json_file:
    age_range_dict = json.load(json_file)

for brand, age_ranges in age_range_dict.items():
    print('\n', brand)
    for age_range in age_ranges['age_range']:
        print('\n', age_range, get_age_group(age_range))