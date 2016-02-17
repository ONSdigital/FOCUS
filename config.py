import json

'''function that will print all key value pairs in a nested dictionary'''
def print_kv(input_data):

        for key, value in input_data.items():

            if isinstance(value, dict):
                print(key)
                print_kv(value)
            elif isinstance(value, int) or isinstance(value, str):
                print(key, value)
            elif isinstance(value, list):
                for item in value:
                    print(item)

def update_v(input_data, find_key, new_value):

        for key, value in input_data.items():

            if isinstance(value, dict):

                if find_key in value:
                    '''replace'''
                    value[find_key] = new_value
                    #
                else:
                    '''next dict'''
                    update_v(value, find_key, new_value)

'''cycle through all the hh'''
def create_households(input_dict):

    for key, value in input_dict.items():
        num = 0
        print(key)
        for i in range(input_dict[key]['number']):
            num += 1

        print(num)

'''creates a new json file with new run id's'''

def generate_config_file(input_data, attribute, new_value): # att/val could be a dict of things to change fed by a nested list

    for key, value in input_data.items():
        if isinstance(value, dict):
            if attribute in value:
                value[attribute] = new_value
            else:
                generate_config_file(value, attribute, new_value)
        elif key == attribute:
            # assign the default value to a new dict unless this is true then change it
            input_data[attribute] = new_value


#file_name = input('Enter file name: ')
#if len(file_name) < 1:
#    file_name = 'default single.JSON'

#    new_data = {}

   # id_num = 1
 #   out_file = open('resp and help variations.JSON', "a")

  #  for i in range(5, 100, 5):
 #       with open(file_name) as data_file:
 #           data = json.load(data_file)  # dict of the whole file

        # change the top level id to match the current id_num
 #       data[str(id_num)] = data.pop('1')

 #       generate_config_file(data[str(id_num)], 'default_resp', i)

 #       new_data[str(id_num)] = data[str(id_num)]

  #      id_num += 1

 #  json.dump(new_data, out_file, sort_keys=True, indent=4)

  #  out_file.close()

#########################################

file_name = input('Enter file name: ')
if len(file_name) < 1:
    file_name = 'level1.JSON'

    new_data = {}

    id_num = 1
    out_file = open('level2.JSON', "a")

    with open(file_name) as data_file:
                data = json.load(data_file)  # dict of the whole file

    list_of_runs = sorted(list(data.keys()), key=int)

    for run in list_of_runs:

        for i in range(5, 100, 5):

            with open(file_name) as data_file:
                data = json.load(data_file)  # dict of the whole file

                # change the top level id to match the current id_num
                data[str(id_num)] = data.pop(run)

                generate_config_file(data[str(id_num)], 'paper_prop', i)

                new_data[str(id_num)] = data[str(id_num)]

                id_num += 1

    json.dump(new_data, out_file, sort_keys=True, indent=4)

    out_file.close()



