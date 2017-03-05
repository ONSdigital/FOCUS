"""A temporary file used to hold scripts that creates input files in the right formats based on a template
and some basic input data"""

import json
import copy
import os
import csv
import math
from collections import defaultdict
from geopy.distance import great_circle
import datetime


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
#input_path = os.path.join(os.getcwd(), 'inputs', 'single multi district.JSON')  # JSON template to use
#new_districts = os.path.join(os.getcwd(), 'inputs', 'management areas(small).csv')  # csv input file with management areas
#output_path = os.path.join(os.getcwd(), 'inputs', 'management areas(small).JSON')  # output JSON file


###generate_multiple_districts_runs(input_path, new_districts, output_path, [[1612, 1312, 725, 487, 362]])


def next_nearest_LSOA(input_lsoa, input_long, input_lat, input_list):
    # looks down the list and selects next nearest LSOA that hasn't been picked already

    temp_dict = {}

    for row in input_list:
        #if not row[4] in read_list:
            # calc dist
            current_lsoa = (input_lat, input_long)
            next_lsoa = (row[3], row[2])

            dist = great_circle(current_lsoa, next_lsoa).kilometers
            temp_dict[row[5]] = dist

    if temp_dict:
        temp_lsoa = min(temp_dict, key=temp_dict.get)
        print("current ", input_lsoa, "next ", temp_lsoa, temp_dict[temp_lsoa], " time ", datetime.datetime.now().time())

    else:
        return 0

    for i, row in enumerate(input_list):
        if row[5] == temp_lsoa:
            return i


def sum_dict(input_dict):

    sum = 0

    for LA in input_dict:
        for LSOA in input_dict[LA]:
            sum += int(input_dict[LA][LSOA])

    return sum


def generate_cca_JSON(input_JSON, input_path, output_path,  hh_per_co=[]):
    # used to create a JSON file showing numbers of each type of hh in each CCA in each LA/LSOA

    # open JSON template file
    with open(input_JSON) as data_file:
        input_data = json.load(data_file)

    # get district template, list of districts and households
    my_district_dict = input_data["1"]["districts"]
    list_of_current_districts = sorted(list(my_district_dict.keys()), key=str)
    district_template = input_data["1"]["districts"][list_of_current_districts[0]]  # template for district

    my_hh_dict = input_data["1"]["districts"][list_of_current_districts[0]]["households"]
    list_of_current_hh = sorted(list(my_hh_dict.keys()), key=str)  # household types

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

    # then run through each row adding hh to CCA makeup entry
    for row in cca_data:

        if not row[0] in input_data[run_counter]['districts']:
            # cca not in list so add and zero area
            input_data[run_counter]['districts'][row[0]] = copy.deepcopy(district_template)
            input_data[run_counter]['districts'][row[0]]['district_area'] = 0

        hh_key = "htc" + row[4]
        if not 'cca_makeup' in input_data[run_counter]['districts'][row[0]]['households'][hh_key]:
            # hh type not in cca so add
            input_data[run_counter]['districts'][row[0]]['households'][hh_key]['cca_makeup'] = defaultdict(dict)

        input_data[run_counter]['districts'][row[0]]['households'][hh_key]['cca_makeup'][row[1]][row[2]] = row[3]
        input_data[run_counter]['districts'][row[0]]['district_area'] += float(row[5])

    list_of_new_districts = sorted(list(input_data[run_counter]['districts'].keys()), key=str)

    for distr in list_of_new_districts:
        co_number = 0
        for hh_type in input_data[run_counter]['districts'][distr]['households']:
            # check if any of this type of hh exist in this cca via looking for cca makeup key
            if 'cca_makeup' in input_data[run_counter]['districts'][distr]['households'][hh_type]:
                # number needs to equal sum of lowest level of cca dict...
                input_data[run_counter]['districts'][distr]['households'][hh_type]['number'] = \
                    sum_dict(input_data[run_counter]['districts'][distr]['households'][hh_type]['cca_makeup'])

                # and calc the number of CO to add based on ratios..
                co_number += input_data[run_counter]['districts'][distr]['households'][hh_type]['number']/\
                            hh_per_co[int(hh_type[-1])-1]

        co_number = math.ceil(co_number)
        # could split between early and late here??
        input_data[run_counter]['districts'][distr]['census officer']['standard']['number'] = co_number

            # output a JSON file
    with open(os.path.join(output_path), 'w') as outfile:
            json.dump(input_data, outfile, indent=4, sort_keys=True)


def remainder_over(cca_output, cca, la_code, lsoa_code, hh_remaining, htc, area, input_ratio, current_co=0):

    current_co += hh_remaining / input_ratio[htc-1]

    if current_co < 12:
        # pass return what to put in cca
        return [[cca, la_code, lsoa_code, hh_remaining, htc, area], current_co]
    else:

        # when adding a proportion of an hh also return proportion of area?

        proportion_over = current_co - 12
        hh_over = math.floor(proportion_over * input_ratio[htc-1])
        hh_to_add = hh_remaining - hh_over
        area_to_add = area*(hh_to_add/hh_remaining)
        cca_output.append([cca, la_code, lsoa_code, hh_to_add, htc, area_to_add])

        # get remainder here and check if that would be over as well
        cca += 1
        current_co = 0

        area_to_carry_over = area - area_to_add

        return remainder_over(cca_output, cca, la_code, lsoa_code, hh_over, htc, area_to_carry_over, input_ratio, current_co)


def create_cca_data(input_path, output_path, input_ratios=[]):

    # creates from the raw data - eventually the address register - a summary breakdown of CCAs ready for conversion
    # to JSON format

    # simple ratio - number of hh per CO
    cca = 1
    cca_output = []
    cca_output_check = []
    current_lsoa = ""

    # load file
    # read in the csv cca data
    with open(input_path, 'r') as f:
        reader = csv.reader(f)
        next(reader)

        raw_data = list(reader)

    current_co = 0

    # get first row LSOA and jump to that point

    # start on row 0
    next_lsoa = 0
    iterations = len(raw_data)

    for i in range(0, iterations):

        #if not raw_data[next_lsoa][4] in cca_output_check:
            row = raw_data[next_lsoa]

    #for row in raw_data:
        #if not row[4] in cca_output_check:
        # for new row need to test if next LSOA is near enough???
        # get next nearest to start
        # then add a limit and if over this start again...

            la_code = row[0]
            la_name = row[1]
            longitude = float(row[2])
            latitude = float(row[3])
            lsoa_code = row[4]
            lsoa_name = row[5]
            hh = int(row[6])
            htc = int(row[7])
            area = float(row[8])


            # delete current here as have required values?
            # delete lsoa already added
            for j, row in enumerate(raw_data):
                if row[5] == lsoa_name:
                    del raw_data[j]
                    break


            #cca_output_check.append(lsoa_code)

            hh_to_add = remainder_over(cca_output, cca, la_code, lsoa_code, hh, htc, area, input_ratios, current_co)
            cca_output.append(hh_to_add[0])
            current_co = hh_to_add[1]  # this is number of CO carried over so if this is > 0 need to find next nearest LSOA...
            cca = hh_to_add[0][0]

            # returns the name of the next nearest lsoa
            next_lsoa = (next_nearest_LSOA(lsoa_name, longitude, latitude, raw_data))


            # then search for the index of the next lsoa
            #for z, row in enumerate(raw_data):
            #    if row[5] == next_lsoa:
            #        next_lsoa = z
            #        break

                    # record the order they are added and visualise?




    with open(output_path, "w") as f:
        # add a header
        writer = csv.writer(f)
        writer.writerow(["CCA", "LA", "LSOA", "number", "htc", "area"])
        writer.writerows(cca_output)


# below sets input and output paths for creation of CCA csv summary
#ratios = [1612, 1312, 725, 487, 362]
ratios = [1200, 1000, 600, 375, 280]
#input_csv_path = os.path.join(os.getcwd(), 'inputs', 'LSOA_hhs_div20.csv')
#output_csv_path = os.path.join(os.getcwd(), 'inputs', 'CCA_all_div20.csv')
#create_cca_data(input_csv_path, output_csv_path, ratios)

# below set input and output paths for creation of JSON file from CSV summary
input_JSON_template = os.path.join(os.getcwd(), 'inputs', 'template.JSON')  # JSON template to use
simple_input_path = os.path.join(os.getcwd(), 'inputs', 'CCA_small.csv')
output_JSON_path = os.path.join(os.getcwd(), 'inputs', 'CCA_small.JSON')
generate_cca_JSON(input_JSON_template, simple_input_path, output_JSON_path, ratios)










