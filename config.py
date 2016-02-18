import json
import copy
from shutil import copyfile

'''creates a new json file with new run id's. Bit of a hack here as need to adjust code to go to different trees
 will need something more swish for the final version with GUI.'''
def generate_config_file(input_data, attribute, new_value): # att/val could be a dict of things to change fed by a nested list

    for key, value in input_data.items():
        if isinstance(value, dict):
            if attribute in value and attribute == 'default_resp':
                # change the alt help too
                value[attribute] = new_value
                value['alt_help'] = new_value + value['default_help']
            elif attribute in value:
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
        # makes the changes to the files but only for the simple case of 1 type of household!
        # to work for all will need to move this test into generate_config_file perhaps
        # return a false and if detected skip it....
        if i != run_data['households']['htc1'][change]:
            sub_data[str(id_num)] = copy.deepcopy(run_data)
            new_data[str(id_num)] = generate_config_file(sub_data[str(id_num)], change, i)
            print(id_num)
            id_num += 1
        # copies the initial entry but otherwise prevents duplicates
        elif i == run_data['households']['htc1'][change] and id_num == 1:
            run_data['households']['htc1']['alt_help'] = run_data['households']['htc1']['default_help'] +\
                                                         run_data['households']['htc1']['default_resp']
            new_data[str(id_num)] = copy.deepcopy(run_data)


            id_num += 1

    #sort the output by key?


    json.dump(new_data, out_file, sort_keys=True, indent=4)

    out_file.close()
    return id_num

changes = {'paper_prop': {'lower': 10,
                          'upper': 70,
                          'step': 20},
           'default_resp': {'lower': 30,
                            'upper': 70,
                            'step': 10}
           }

src_file_name = input('Enter file name: ')
if len(src_file_name) < 1:
    src_file_name = 'default single.JSON'

dst_file_name = input('Enter file name: ')
if len(dst_file_name) < 1:
    dst_file_name = 'default.JSON'

copyfile(src_file_name, dst_file_name)

new_data = {}
id_num = 1

out_file = dst_file_name

list_of_changes = list(changes.keys())

for change in list_of_changes:

    with open(out_file) as data_file2:
        data2 = json.load(data_file2)  # dict of the whole file

    list_of_runs = sorted(list(data2.keys()), key=int)

    for run in list_of_runs:

        # pass into a function here the run to use function returns new input file
        id_num = generate_output_file(data2[run], run, id_num, change, changes[change], out_file)



