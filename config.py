import json
import copy

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
            value[attribute] = new_value

    return input_data

def generate_output_file(run_data, run, id_num, change, changes, out_file):

    sub_data = {}
    lower = changes['lower']
    upper = changes['upper']
    step = changes['step']

    out_file = open(out_file, "w")

    for i in range(lower, upper, step):
        if i != run_data['households']['htc1'][change]:
            sub_data[str(id_num)] = copy.deepcopy(run_data)
            new_data[str(id_num)] = generate_config_file(sub_data[str(id_num)], change, i)
            id_num += 1
        elif i == run_data['households']['htc1'][change] and id_num == 1:
            new_data[str(id_num)] = copy.deepcopy(run_data)
            id_num += 1

    json.dump(new_data, out_file, sort_keys=True, indent=4)
    out_file.close()
    return id_num

changes = {'paper_prop': {'lower': 5,
                          'upper': 15,
                          'step': 5},
           'default_resp': {'lower': 5,
                          'upper': 15,
                          'step': 5},
           'another': {'lower': 5,
                          'upper': 15,
                          'step': 5}
           }

file_name = input('Enter file name: ')
if len(file_name) < 1:
    file_name = 'default single copy.JSON'

    new_data = {}
    id_num = 1

    out_file = 'default single copy.JSON'

    list_of_changes = list(changes.keys())

    for change in list_of_changes:

        with open(out_file) as data_file2:
            data2 = json.load(data_file2)  # dict of the whole file

        list_of_runs = sorted(list(data2.keys()), key=int)

        for run in list_of_runs:

            # pass into a function here the run to use function returns new input file
            id_num = generate_output_file(data2[run], run, id_num, change, changes[change], out_file)




#############################################################################



#file_name = input('Enter file name: ')
#if len(file_name) < 1:
 #   file_name = 'default single.JSON'

  #  new_data = {}

#    id_num = 1
  #  out_file = open('level1.JSON', "a")

  #  with open(file_name) as data_file:
   #             data = json.load(data_file)  # dict of the whole file

 #   list_of_runs = sorted(list(data.keys()), key=int)

 #   for run in list_of_runs:

  #      sub_data = {}

   #     for i in range(5, 15, 5):

   #         sub_data[str(id_num)] = copy.deepcopy(data[run])
   #         generate_config_file(sub_data[str(id_num)], 'paper_prop', i)
   #         new_data[str(id_num)] = sub_data[str(id_num)]
   #         id_num += 1

  #  json.dump(new_data, out_file, sort_keys=True, indent=4)

   # out_file.close()






