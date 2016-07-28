
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
    with open(all_district_list, 'r') as f:

        reader = csv.reader(f)
        next(reader)
        my_area_data = list(reader)

    for district in list_of_current_districts:
        del input_data["1"]["districts"][district]

    for row in my_area_data:
        if row[0] in specified_district:

            co_number = 0
            #district = row[0]
            district = row[2]
            input_data["1"]["districts"][district] = copy.deepcopy(district_template)
            # then populate each sub dictionary appropriately
            # number of households
            hh_count = 0

            for HH in list_of_current_hh:
                hh_number = int(row[int(HH[-1])+2])  # number as in new district list input
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
            #input_data["1"]["districts"][district]["district_area"] = float(row[7])
            input_data["1"]["districts"][district]["district_area"] = float(row[8])
            input_data["1"]["districts"][district]["LA"] = str(row[0])

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

    # inpout data by here is the base without the dist
    run_template = input_data["1"]

    run_counter = 1

    for ratios in hh_per_co:

        input_data[str(run_counter)] = copy.deepcopy(run_template)

        for row in my_area_data:

            co_number = 0
            hh_count = 0
            #district = row[0]
            district = row[2]
            input_data[str(run_counter)]["districts"][district] = copy.deepcopy(district_template)
            # then populate each sub dictionary appropriately
            # number of households
            for HH in list_of_current_hh:
                hh_number = int(row[int(HH[-1])+2])  # number as in new district list input
                hh_count += hh_number

                if hh_number == 0:
                    # delete entry
                    del input_data[str(run_counter)]["districts"][district]["households"][HH]
                else:
                    input_data[str(run_counter)]["districts"][district]["households"][HH]["number"] = int(hh_number)
                    co_number += hh_number/ratios[int(HH[-1])-1]

                    # update CO speed according to area? So do simple travel calc and if average above X set to driving?
            area = float(row[8])
            hh_area = area / hh_count
            hh_sep = 2 * (math.sqrt(hh_area / math.pi))
            est_travel_time = (hh_sep / 5) * 60

            if est_travel_time > 10:
                # give them a car and increase numbers!
                input_data[str(run_counter)]["districts"][district]["census officer"]["standard"]["travel_speed"] = 40
                co_number = co_number*3

            elif est_travel_time > 5:
                # give them a bike!
                input_data[str(run_counter)]["districts"][district]["census officer"]["standard"]["travel_speed"] = 10
                co_number = co_number * 1.5

            input_data[str(run_counter)]["districts"][district]["census officer"]["standard"]["number"] = int(math.ceil(co_number))
            #input_data[str(run_counter)]["districts"][district]["district_area"] = float(row[7])
            input_data[str(run_counter)]["districts"][district]["district_area"] = float(row[8])
            input_data[str(run_counter)]["districts"][district]["LA"] = str(row[0])

        run_counter += 1

        # dump as new json file
    with open(os.path.join(output_JSON_name), 'w') as outfile:
        json.dump(input_data, outfile,  indent=4, sort_keys=True)


input_path = os.path.join(os.getcwd(), 'inputs', 'single multi district.JSON')
new_districts = os.path.join(os.getcwd(), 'inputs', 'CCA_hh.csv')
output_path = os.path.join(os.getcwd(), 'inputs', 'all_LA_hh.JSON')
spec_output_path = os.path.join(os.getcwd(), 'inputs', 'spec_LA_hh.JSON')

#generate_multiple_districts(input_path, new_districts, output_path, [1290, 1050, 580, 390, 290])



#generate_test_file('inputs/all_LA_hh.JSON', 'inputs/test_LA_hh.JSON', 50)

#generate_multiple_districts_runs(input_path, new_districts, output_path, [[2580, 2100, 1160, 780, 580],
#                                                                          [2257, 1837, 1015, 682, 507],
#                                                                          [1935, 1575, 870, 585, 435],
#                                                                          [1612, 1312, 725, 487, 362],
#                                                                          [1290, 1050, 580, 390, 290],
#                                                                          [967, 787, 435, 292, 217],
#                                                                          [645, 525, 290, 195, 145],
#                                                                          [322, 262, 145, 97, 72]])

generate_multiple_districts_runs(input_path, new_districts, output_path, [[1290, 1050, 580, 390, 290]])

# 2017 test
#generate_specified_districts(input_path, new_districts, spec_output_path, ['E08000016',
#                                                                           'E08000019',
#                                                                           'W06000023',
#                                                                           'E07000189',
#                                                                           'E07000052',
#                                                                           'E09000028'])

generate_specified_districts(input_path, new_districts, spec_output_path, ['E07000026',
                                                                           'E07000032',
                                                                           'E07000170',
                                                                           'E07000223',
                                                                           'E07000224'
                                                                           ])




