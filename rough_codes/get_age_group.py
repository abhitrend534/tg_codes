import json

def get_my(age_range):
    return age_range[0][-1]

def get_age_group(age_range):
    my = get_my(age_range)
    if my == 'm':
        if age_range[-1] in ['7m', '8m', '9m', '10m', '11m', '12m', '13m', '14m', '15m', '16m', '17m', '18m', '19m', '20m', '21m', '22m', '23m', '24m']:
            return 'baby'
        elif age_range[-1] in ['0m', '1m', '2m', '3m', '4m', '5m', '6m']:
            return 'new_born'
        else:
            return 'baby'
    elif my == 'y':
        if age_range == ['18y']:
            return 'adult'
        elif age_range[-1] in ['13y', '14y', '15y', '16y', '17y', '18y']:
            return 'teen'
        elif age_range[-1] in ['8y', '9y', '10y', '11y', '12y']:
            return 'senior'
        elif age_range[-1] in ['0y', '0.75y', '1y', '1.5y', '2y', '3y', '4y', '5y', '6y', '7y']:
            return 'junior'
    else:
        return 'unsupported age_range'

read_file = 'unique_age_ranges.json'

with open(read_file, 'r') as json_file:
    age_range_dict = json.load(json_file)

for brand, age_ranges in age_range_dict.items():
    print('\n', brand)
    for age_range in age_ranges['age_range']:
        print('\n', age_range, get_age_group(age_range))