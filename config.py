
import json
import copy
import os
import csv
import math
from collections import defaultdict


# generate new runs from a template and source csv
def generate_multiple_districts_runs(input_JSON, new_district_list, output_JSON_name, hh_per_co = []):
    # read in JSON file

    with open(input_JSON) as data_file:
        input_data = json.load(data_file)

    # get list of keys in district area
    my_district_dict = input_data["1"]["districts"]
    list_of_current_districts = sorted(list(my_district_dict.keys()), key=str)
    district_template = input_data["1"]["districts"][list_of_current_districts[0]]

    my_hh_dict = input_data["1"]["districts"][list_of_current_districts[0]]["households"]
    list_of_current_hh = sorted(list(my_hh_dict.keys()), key=str)

    with open(new_district_list, 'r') as f:

        reader = csv.reader(f)
        next(reader)

        my_area_data = list(reader)

    for district in list_of_current_districts:
        del input_data["1"]["districts"][district]

    # input data by here is the base without the dist
    run_template = input_data["1"]

    run_counter = 1

    for ratios in hh_per_co:

        input_data[str(run_counter)] = copy.deepcopy(run_template)

        for row in my_area_data:

            # if CCA split across LA/LSOA need to check here if CCA already added and add new hh to same district
            # but HH will have ID which shows they belong to a different  LA LSOA...

            co_number = 0
            hh_count = 0
            district = row[0]
            input_data[str(run_counter)]["districts"][district] = copy.deepcopy(district_template)
            # then populate each sub dictionary appropriately
            # number of households
            for HH in list_of_current_hh:
                hh_number = int(row[int(HH[-1])+1])  # number as in new district list input
                hh_count += hh_number

                if hh_number == 0:
                    # delete entry
                    del input_data[str(run_counter)]["districts"][district]["households"][HH]
                else:
                    if ratios[int(HH[-1])-1] == 0:

                        input_data[str(run_counter)]["districts"][district]["households"][HH]["number"] = int(hh_number)
                        co_number = 0

                    else:

                        input_data[str(run_counter)]["districts"][district]["households"][HH]["number"] = int(hh_number)
                        co_number += hh_number/ratios[int(HH[-1])-1]

            # update CO speed according to area? So do simple travel calc and if average above X set to driving?
            area = float(row[7])
            hh_area = area / hh_count
            hh_sep = 2 * (math.sqrt(hh_area / math.pi))
            est_travel_time = (hh_sep / 5) * 60

            if est_travel_time > 10:
                # give them a car and increase the numbers!
                input_data[str(run_counter)]["districts"][district]["census officer"]["standard"]["travel_speed"] = 40
                #co_number = co_number*3

            elif est_travel_time > 5:
                # give them a bike and increase the numbers!
                input_data[str(run_counter)]["districts"][district]["census officer"]["standard"]["travel_speed"] = 10
                #co_number = co_number * 1.5

            input_data[str(run_counter)]["districts"][district]["census officer"]["standard"]["number"] = int(math.ceil(co_number))
            print(district, " ", int(math.ceil(co_number)))
            #input_data[str(run_counter)]["districts"][district]["district_area"] = float(row[7])
            input_data[str(run_counter)]["districts"][district]["district_area"] = float(row[7])
            #input_data[str(run_counter)]["districts"][district]["LA"] = str(row[0]) # move to households...

        run_counter += 1

        # dump as new json file
    with open(os.path.join(output_JSON_name), 'w') as outfile:
        json.dump(input_data, outfile,  indent=4, sort_keys=True)

# set paths to use
input_path = os.path.join(os.getcwd(), 'inputs', 'single multi district.JSON')  # JSON template to use
new_districts = os.path.join(os.getcwd(), 'inputs', 'management areas(small).csv')  # csv input file with management areas
output_path = os.path.join(os.getcwd(), 'inputs', 'management areas(small).JSON')  # output JSON file


###generate_multiple_districts_runs(input_path, new_districts, output_path, [[1612, 1312, 725, 487, 362]])

def sum_dict(input_dict):

    sum = 0

    for LA in input_dict:
        for LSOA in input_dict[LA]:
            sum += int(input_dict[LA][LSOA])

    return sum


def generate_cca_makeup(input_JSON, input_path, output_path,  hh_per_co = []):
    # used to create a JSON file showing numbers of each type of hh in EA in each LA/LSOA

    # open JSON template file
    with open(input_JSON) as data_file:
        input_data = json.load(data_file)

    # get district template, list of districts and households
    my_district_dict = input_data["1"]["districts"]
    list_of_current_districts = sorted(list(my_district_dict.keys()), key=str)
    district_template = input_data["1"]["districts"][list_of_current_districts[0]] #  template for district

    my_hh_dict = input_data["1"]["districts"][list_of_current_districts[0]]["households"]
    list_of_current_hh = sorted(list(my_hh_dict.keys()), key=str) # household types

    # delete the current districts in the template
    for district in list_of_current_districts:
        del input_data["1"]["districts"][district]

    # input data by here is the base without the dist
    run_template = input_data["1"]  # top level template
    run_counter = '1'

    input_data[str(run_counter)] = copy.deepcopy(run_template)

    # read in the csv cca data
    with open(input_path, 'r') as f:
        reader = csv.reader(f)
        next(reader)

        cca_data = list(reader)

    # get unique keys - ie the cca's
    cca_unique = set()
    for row in cca_data:
        cca_unique.add(row[0])

    # then run through each row adding hh
    for row in cca_data:
        if not row[0] in input_data[run_counter]['districts']:
            #cca not in list so add
            input_data[run_counter]['districts'][row[0]] = copy.deepcopy(district_template)

            if not 'cca_makeup' in input_data[run_counter]['districts'][row[0]]['households'][row[1]]:
                # hh type not in cca so add
                input_data[run_counter]['districts'][row[0]]['households'][row[1]]['cca_makeup'] = defaultdict(dict)

        input_data[run_counter]['districts'][row[0]]['households'][row[1]]['cca_makeup'][row[2]][row[3]] = row[4]


    # then for each district add up total households of each type and as you do calc CO' required....
    # add then exit converting to JSON!

    list_of_new_districts = sorted(list(input_data[run_counter]['districts'].keys()), key=str)

    for distr in list_of_new_districts:
        co_number = 0
        for hh_type in input_data[run_counter]['districts'][distr]['households']:
            # check if any of this type of hh exist in this cca via looking for cca makeup key
            if 'cca_makeup' in input_data[run_counter]['districts'][row[0]]['households'][hh_type]:
                # number needs to equal sum of lowest level of cca dict...
                input_data[run_counter]['districts'][distr]['households'][hh_type]['number'] = \
                    sum_dict(input_data[run_counter]['districts'][row[0]]['households'][hh_type]['cca_makeup'])

                # and calc the number of CO to add based on ratios..
                co_number += input_data[run_counter]['districts'][distr]['households'][hh_type]['number']/\
                            hh_per_co[int(hh_type[-1])-1]

        co_number = math.ceil(co_number)
        input_data[run_counter]['districts'][distr]['census officer']['standard']['number'] = co_number





            # output a JSON file
    with open(os.path.join(output_path), 'w') as outfile:
            json.dump(input_data, outfile, indent=4, sort_keys=True)






# if new area create - else skip to existing
# if new LA create - else add to exisit
# if new LSOA add to existing

input_JSON_path = os.path.join(os.getcwd(), 'inputs', 'single multi district.JSON')  # JSON template to use
simple_input_path = os.path.join(os.getcwd(), 'inputs', 'simple_in.csv')
simple_output_path = os.path.join(os.getcwd(), 'inputs', 'simple_out.json')

generate_cca_makeup(input_JSON_path, simple_input_path, simple_output_path, [1612, 1312, 725, 487, 362] )

