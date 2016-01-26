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
def update_run_id(input_file, start_id, find_key, new_value):

    # loads the selected config file
    with open(input_file) as data_file:
        data = json.load(data_file)  # dict of the whole file

        list_of_keys = sorted(list(data.keys()), key=int)  # top level keys only

        for key in list_of_keys:
            data[str(start_id)] = data.pop(key)
            data[str(start_id)]['description'] = 'default'

            '''then go through each entry in the dict until you find the right key then update'''
            update_v(data[str(start_id)], find_key, new_value)

            start_id += 1

        out_file = open('default2.JSON', "w")
        json.dump(data, out_file, indent=4)

# Close the file
        out_file.close()


file_name = input('Enter file name: ')
if len(file_name) < 1:
    file_name = 'data.JSON'

update_run_id(file_name, 1, 'paper_prop', 5)



# loads the selected config file
#with open('data.JSON') as data_file:
#    data = json.load(data_file)  # dict of the whole file

#list_of_keys = sorted(list(data.keys()))  # returns top level - iterate through this list to do all the runs

#for run in list_of_keys:
#    print(run)

 #   run_dict = data[run]  # dict for current run, pass to initialise module
    #print_kv(run_dict)

  #  print('Replications:', run_dict['replications'])  # reps, keep in FOCUS main

   # create_households(run_dict['households'])  # run in initialise module













