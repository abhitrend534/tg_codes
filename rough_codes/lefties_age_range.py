import json

read_file = 'lefties_unique_size_names.json'

with open(read_file, 'r') as json_file:
    size_list = json.load(json_file)

def get_age_group(age_range):
    new_born_ages = ['0m', '1m', '2m', '3m', '4m', '5m', '6m']
    baby_ages = ['7m', '8m', '9m', '10m', '11m', '12m', '13m', '14m', '15m', '16m', '17m', '18m', '19m', '20m', '21m', '22m', '23m', '24m']
    junior_ages = ['2y', '3y', '4y', '5y', '6y', '7y']
    senior_ages = ['8y', '9y', '10y', '11y', '12y']
    teen_ages = ['13y', '14y', '15y', '16y', '17y']
    adult_ages = ['18y']

    age_goup_list = ['new_born', 'baby', 'junior', 'senior', 'teen', 'adult']

    if len(age_range) == 1:
        if age_range[0] in new_born_ages:
            return ['new_born']
        elif age_range[0] in baby_ages:
            return ['baby']
        elif age_range[0] in junior_ages:
            return ['junior']
        elif age_range[0] in senior_ages:
            return ['senior']
        elif age_range[0] in teen_ages:
            return ['teen']
        elif age_range[0] in adult_ages:
            return ['adult']
    else:
        age_group = []
        start = age_range[0]
        end = age_range[-1]

        if start in new_born_ages:
            sindex = age_goup_list.index('new_born')
        elif start in baby_ages:
            sindex = age_goup_list.index('baby')
        elif start in junior_ages:
            sindex = age_goup_list.index('junior')
        elif start in senior_ages:
            sindex = age_goup_list.index('senior')
        elif start in teen_ages:
            sindex = age_goup_list.index('teen')
        elif start in adult_ages:
            sindex = age_goup_list.index('adult')
        
        if end in new_born_ages:
            eindex = age_goup_list.index('new_born')
        elif end in baby_ages:
            eindex = age_goup_list.index('baby')
        elif end in junior_ages:
            eindex = age_goup_list.index('junior')
        elif end in senior_ages:
            eindex = age_goup_list.index('senior')
        elif end in teen_ages:
            eindex = age_goup_list.index('teen')
        elif end in adult_ages:
            eindex = age_goup_list.index('adult')

        for i in range(sindex, eindex + 1):
            age_group.append(age_goup_list[i])

        if age_group == []:
            age_group = ['others']
            
        return age_group

def remap_age_range(age_range):
    if len(age_range) > 1:
        age_range = [age_range[0], age_range[-1]]

    if age_range[0] == '1y':
        if len(age_range) == 1:
            age_range = ['12m']
        else:
            end = age_range[-1]
            age_range = ['12m', end]

    if len(age_range) > 1 and age_range[-1] == '2y':
        end = '24m'
        age_range = ['12m', end]

    if len(age_range) > 1 and age_range[0] == '24m':
        end = str(int(int(age_range[-1][:-1])/12)) + 'y'
        age_range = ['2y', end]

    return age_range

def get_age_range(sizename):
    age_range = []
    age_shortname = sizename.strip()
    if 'months' in age_shortname or 'years' in age_shortname:
        if 'year' in age_shortname:
            age_shortname = sizename.split('y')[0] + 'y'
        elif 'month' in age_shortname:
            age_shortname = sizename.split('m')[0] + 'm' 

        if '1½ y' in age_shortname:
            age_shortname = age_shortname.replace('1½ y', '18 m')
            
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

    age_range = remap_age_range(age_range)
    return age_range

for size in size_list:
    age_range = get_age_range(size)
    age_group = get_age_group(age_range)
    print('\n', size, age_range, age_group)