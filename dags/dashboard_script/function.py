
from dateutil import parser 


# convert date string to date
def date_string_convert_to_date(date):
    
    datetime_str = date

    date_time = parser.parse(datetime_str)
    date_time = date_time.strftime('%Y-%m-%d')
    date_time = parser.parse(date_time)
    return date_time

def date_to_string(date):
    date_date = date
    str_date = date_date.strftime('%Y-%m-%d')
    return str_date

def tuple_to_int(tuple):
    my_strings = [str(x) for x in tuple]
    my_string = "".join(my_strings)
    my_int = int(my_string)   
    return my_int

def get_state_short(state_name):
    state_short = ''
    if state_name == 'Johor':
        state_short = 'jhr'
    elif state_name == 'Kedah':
        state_short = 'kdh'
    elif state_name == 'Kelantan':
        state_short = 'ktn'
    elif state_name == 'Klang Valley':
        state_short = 'kvy'
    elif state_name == 'Wilayah Persekutuan Kuala Lumpur':
        state_short = 'kul'
    elif state_name == 'Wilayah Persekutuan Labuan':
        state_short = 'lbn'
    elif state_name == 'Melaka':
        state_short = 'mlk'
    elif state_name == 'Negeri Sembilan':
        state_short = 'nsn'
    elif state_name == 'Pahang':
        state_short = 'phg'
    elif state_name == 'Perak':
        state_short = 'prk'
    elif state_name == 'Perlis':
        state_short = 'pls'
    elif state_name == 'Pulau Pinang':
        state_short = 'png'
    elif state_name == 'Wilayah Persekutuan Putrajaya':
        state_short = 'pjy'
    elif state_name == 'Sabah':
        state_short = 'sbh'
    elif state_name == 'Sarawak':
        state_short = 'swk'
    elif state_name == 'Selangor':
        state_short = 'sgr'
    elif state_name == 'Terengganu':
        state_short = 'trg'
    return state_short