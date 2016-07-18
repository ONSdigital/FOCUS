
import json
import copy
import os
import csv
import math


def generate_multiple_districts(input_JSON, new_district_list, output_JSON_name, hh_per_co):
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

    # for each row of co ratios?

    for row in my_area_data:

        co_number = 0
        hh_count = 0
        district = row[0]
        input_data["1"]["districts"][district] = copy.deepcopy(district_template)
        # then populate each sub dictionary appropriately
        # number of households
        for HH in list_of_current_hh:
            hh_number = int(row[int(HH[-1])+1])  # number as in new district list input
            hh_count += hh_number

            if hh_number == 0:
                # delete entry
                del input_data["1"]["districts"][district]["households"][HH]
            else:
                input_data["1"]["districts"][district]["households"][HH]["number"] = int(hh_number)
                co_number += hh_number/hh_per_co[int(HH[-1])-1]

                # update CO speed according to area? So do simple travel calc and if average above X set to driving?
        area = float(row[7])
        hh_area = area / hh_count
        hh_sep = 2 * (math.sqrt(hh_area / math.pi))
        est_travel_time = (hh_sep / 5) * 60

        if est_travel_time > 10:
            # give them a bike!
            input_data["1"]["districts"][district]["census officer"]["standard"]["travel_speed"] = 10

        elif est_travel_time > 5:
            # give them a car!
            input_data["1"]["districts"][district]["census officer"]["standard"]["travel_speed"] = 40

        input_data["1"]["districts"][district]["census officer"]["standard"]["number"] = int(math.ceil(co_number))
        input_data["1"]["districts"][district]["district_area"] = float(row[7])

    # dump as new json file
    with open(os.path.join(output_JSON_name), 'w') as outfile:
        json.dump(input_data, outfile,  indent=4, sort_keys=True)


def generate_specified_districts(input_JSON, all_district_list, output_JSON_name, specified_district):
    # read in JSON file
    with open(input_JSON) as data_file:
        input_data = json.load(data_file)

    # get list of keys in district area
    my_district_dict = input_data["1"]["districts"]
    list_of_current_districts = sorted(list(my_district_dict.keys()), key=str)
    district_template = input_data["1"]["districts"][list_of_current_districts[0]]

    my_hh_dict = input_data["1"]["districts"][list_of_current_districts[0]]["households"]
    list_of_current_hh = sorted(list(my_hh_dict.keys()), key=str)

    # numbers of HH per htc hh
    hh_per_co = [1290, 1050, 580, 390, 290]
    #hh_per_co = [1290, 1050, 580, 200, 150]
    #hh_per_co = [500, 250, 250, 100, 75]
    with open(all_district_list, 'r') as f:

        reader = csv.reader(f)
        next(reader)
        my_area_data = list(reader)

    for district in list_of_current_districts:
        del input_data["1"]["districts"][district]

    for row in my_area_data:
        if row[0] in specified_district:

            co_number = 0
            district = row[0]
            input_data["1"]["districts"][district] = copy.deepcopy(district_template)
            # then populate each sub dictionary appropriately
            # number of households
            hh_count = 0

            for HH in list_of_current_hh:
                hh_number = int(row[int(HH[-1])+1])  # number as in new district list input
                hh_count += hh_number

                if hh_number == 0:
                    # delete entry
                    del input_data["1"]["districts"][district]["households"][HH]
                else:
                    input_data["1"]["districts"][district]["households"][HH]["number"] = int(hh_number)
                    co_number += hh_number/hh_per_co[int(HH[-1])-1]

            # update CO speed according to area? So do simple travel calc and if average above X set to driving?
            area = float(row[7])
            hh_area = area/hh_count
            hh_sep = 2 * (math.sqrt(hh_area / math.pi))
            est_travel_time = (hh_sep/5)*60

            if est_travel_time > 10:
                # give them a bike!
                input_data["1"]["districts"][district]["census officer"]["standard"]["travel_speed"] = 40

            elif est_travel_time > 5:
                # give them a car!
                input_data["1"]["districts"][district]["census officer"]["standard"]["travel_speed"] = 10

            input_data["1"]["districts"][district]["census officer"]["standard"]["number"] = int(math.ceil(co_number))
            input_data["1"]["districts"][district]["district_area"] = float(row[7])

    # dump as new json file
    with open(os.path.join(output_JSON_name), 'w') as outfile:
        json.dump(input_data, outfile,  indent=4, sort_keys=True)


def generate_test_file(input_JSON, output_JSON_name, number):

    with open(input_JSON) as data_file:
        input_data = json.load(data_file)

    list_of_districts = sorted(list(input_data['1']['districts']))
    count = 1

    for distr in list_of_districts:
        if count > number:
            input_data['1']['districts'].pop(distr, None)

        count += 1

    with open(os.path.join(output_JSON_name), 'w') as outfile:
        json.dump(input_data, outfile,  indent=4, sort_keys=True)


def generate_multiple_districts_runs(input_JSON, new_district_list, output_JSON_name, hh_per_co):
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

    # for each row of co ratios?

    for row in my_area_data:

        co_number = 0
        hh_count = 0
        district = row[0]
        input_data["1"]["districts"][district] = copy.deepcopy(district_template)
        # then populate each sub dictionary appropriately
        # number of households
        for HH in list_of_current_hh:
            hh_number = int(row[int(HH[-1])+1])  # number as in new district list input
            hh_count += hh_number

            if hh_number == 0:
                # delete entry
                del input_data["1"]["districts"][district]["households"][HH]
            else:
                input_data["1"]["districts"][district]["households"][HH]["number"] = int(hh_number)
                co_number += hh_number/hh_per_co[int(HH[-1])-1]

                # update CO speed according to area? So do simple travel calc and if average above X set to driving?
        area = float(row[7])
        hh_area = area / hh_count
        hh_sep = 2 * (math.sqrt(hh_area / math.pi))
        est_travel_time = (hh_sep / 5) * 60

        if est_travel_time > 10:
            # give them a bike!
            input_data["1"]["districts"][district]["census officer"]["standard"]["travel_speed"] = 10

        elif est_travel_time > 5:
            # give them a car!
            input_data["1"]["districts"][district]["census officer"]["standard"]["travel_speed"] = 40

        input_data["1"]["districts"][district]["census officer"]["standard"]["number"] = int(math.ceil(co_number))
        input_data["1"]["districts"][district]["district_area"] = float(row[7])

    # dump as new json file
    with open(os.path.join(output_JSON_name), 'w') as outfile:
        json.dump(input_data, outfile,  indent=4, sort_keys=True)




input_path = os.path.join(os.getcwd(), 'inputs', 'single multi district.JSON')
new_districts = os.path.join(os.getcwd(), 'inputs', 'LA_hh.csv')
output_path = os.path.join(os.getcwd(), 'inputs', 'all_LA_hh.JSON')
spec_output_path = os.path.join(os.getcwd(), 'inputs', 'spec_LA_hh.JSON')

generate_multiple_districts(input_path, new_districts, output_path, [1290, 1050, 580, 390, 290])
#generate_specified_districts(input_path, new_districts, spec_output_path, ['E06000054',
#                                                                           'E09000009',
#                                                                           'E09000020',
#                                                                           'E07000094'])
generate_specified_districts(input_path, new_districts, spec_output_path, ['E09000020'])

generate_test_file('inputs/all_LA_hh.JSON', 'inputs/test_LA_hh.JSON', 50)
#changes = [[['districts', 'all', 'trigger'], 80], [['districts', 'all', 'RMT_update'], 48]]
#generate_multiple_runs('inputs/test_LSOA_hh.JSON', changes)
#generate_test_file('inputs/LA_hh.JSON', 'inputs/small_test_LA_hh.JSON', 3)



