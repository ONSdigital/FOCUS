
import json
import copy
import os
import csv
import math


def generate_multiple_districts(input_JSON, new_district_list, output_JSON_name):
    # read in JSON file
    with open(input_JSON) as data_file:
        input_data = json.load(data_file)

    # get list of keys in district area
    my_district_dict = input_data["1"]["districts"]
    list_of_current_districts = sorted(list(my_district_dict.keys()), key=str)
    district_template = input_data["1"]["districts"][list_of_current_districts[0]]

    my_hh_dict = input_data["1"]["districts"][list_of_current_districts[0]]["households"]
    list_of_current_hh = sorted(list(my_hh_dict.keys()), key=str)

    hh_per_co = [1290, 1050, 580, 390, 290]

    with open(new_district_list, 'r') as f:

        reader = csv.reader(f)
        next(reader)

        my_area_data = list(reader)

    for district in list_of_current_districts:
        del input_data["1"]["districts"][district]

    for row in my_area_data:

        co_number = 0
        district = row[0]
        input_data["1"]["districts"][district] = copy.deepcopy(district_template)
        # then populate each sub dictionary appropriately
        # number of households
        for HH in list_of_current_hh:
            hh_number = int(row[int(HH[-1])+1])  # number as in new district list input

            if hh_number == 0:
                # delete entry
                del input_data["1"]["districts"][district]["households"][HH]
            else:
                input_data["1"]["districts"][district]["households"][HH]["number"] = int(hh_number)
                co_number += hh_number/hh_per_co[int(HH[-1])-1]



        input_data["1"]["districts"][district]["census officer"]["walking"]["number"] = int(math.ceil(co_number))
        input_data["1"]["districts"][district]["district_area"] = float(row[7])

    # dump as new json file
    with open(os.path.join(output_JSON_name), 'w') as outfile:
        json.dump(input_data, outfile,  indent=4, sort_keys=True)


input_path = os.path.join(os.getcwd(), 'inputs', 'single multi district.JSON')
new_districts = os.path.join(os.getcwd(), 'inputs', 'test_LA_hh.csv')
output_path = os.path.join(os.getcwd(), 'inputs', 'test_LA_hh.JSON')

generate_multiple_districts(input_path, new_districts, output_path)




