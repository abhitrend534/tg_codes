import json

with open('usa_unique_sizes.json', 'r') as file:
    sizes = json.load(file)

def is_float(s):
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False
    
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
    if len(age_range) == 2 and age_range[1] == '2y':
        fi = int(float(age_range[0].replace('y', '')) * 12)
        si = int(float(age_range[1].replace('y', '')) * 12)
        age_range = [str(fi) + 'm', str(si) + 'm']
    elif len(age_range) == 2 and age_range[0] == '24m':
        fi = int(float(age_range[0].replace('m', '')) / 12)
        si = int(float(age_range[1].replace('m', '')) / 12)
        age_range = [str(fi) + 'y', str(si) + 'y']

    if age_range[1] == '36m':
        age_range = [age_range[0], '3y']
    if age_range[0] == '1y':
        age_range = ['12m', age_range[1]]
    if age_range == ['2y']:
        age_range = ['24m']
    return age_range
    
def get_age_range(size):
    size = size.replace(' ', '')
    if 'months' in size:
        if '-' in size:
            tsize = size.replace('months', '')
            srange = tsize.split('-')
            age_range = [srange[0] + 'm', srange[1] + 'm']
            age_range = remap_age_range(age_range)
            return age_range
        else:
            size = size.replace('months', 'm')
            return [size]
    elif 'years' in size:
        if '-' in size:
            tsize = size.replace('years', '')
            srange = tsize.split('-')
            age_range = [srange[0] + 'y', srange[1] + 'y']
            age_range = remap_age_range(age_range)
            return age_range
        else:
            size = size.replace('years', 'y')
            age_range = remap_age_range([size])
            return age_range
    elif '-' in size:
        size1 = size.split('-')[0].strip()
        size2 = size.split('-')[1].strip()
        if is_float(size1) and is_float(size2):
            age_range = [size1 + 'm', size2 + 'm']
            age_range = remap_age_range(age_range)
            return age_range
    return None

for sizename in sizes:
    if '-' in sizename and '½' not in sizename:
        age_range = get_age_range(sizename)
        if age_range:
            age_group = get_age_group(age_range)
            print(sizename, age_range, age_group)