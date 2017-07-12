"""A temporary file used to hold scripts that creates input files in the right formats based on a template
and some basic input data"""

import json
import copy
import os
import csv
import math
from collections import defaultdict
import datetime as dt
import pandas as pd
from math import sin, cos, sqrt, atan2, radians
import glob
import ntpath



def calc_dist(inlat1, inlat2, inlong1, inlong2):
    """calculates distance bewtween two latitudes and longitudes taking earths curvature into account"""
    # approximate radius of earth in km
    R = 6373.0

    lat1 = radians(inlat1)
    lon1 = radians(inlong1)
    lat2 = radians(inlat2)
    lon2 = radians(inlong2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c


def generate_nomis_cca():
    """takes raw nomis data of form:

    LSOA11CD    hh_type1 hh_type2 ...
    E01011949     120	    333
    E01011950     122	    296

    area data of form:

    LSOA11CD	AREAKM	LAD11CD	    LAD11NM
    E01011949	0.5189	E06000001	Hartlepool
    E01011950	0.1325	E06000001	Hartlepool

    and Latitude and longitudes in the form:

    lsoa11cd	long	        lat
    E01012007	-1.242212348	54.5460388
    E01012085	-1.201874496	54.55081431

    and produces a flat csv file that details the makeup of E&W at household level
    """

    nomis_df = pd.read_csv(os.path.join(os.getcwd(), 'inputs', 'NOMIS_lsoa_lower.csv'))

    # parse first col into two
    nomis_df['lsoa11cd'] = nomis_df['2011 super output area - lower layer'].str.split(':').str[0]
    nomis_df['lsoa11nm'] = nomis_df['2011 super output area - lower layer'].str.split(':').str[1]
    # remove any whitespaces
    nomis_df['lsoa11cd'] = nomis_df['lsoa11cd'].str.strip()
    nomis_df['lsoa11nm'] = nomis_df['lsoa11nm'].str.strip()
    # drop the original column
    nomis_df.drop(['2011 super output area - lower layer'], axis=1, inplace=True)

    # rename cols
    codes = [str(i) for i in range(1, 16)] + ['lsoa11cd', 'lsoa11nm']
    nomis_df.columns = codes

    # flatten and sort df
    nomis_flat_df = pd.melt(nomis_df, id_vars=['lsoa11cd', 'lsoa11nm'], value_vars=codes[:-2])
    nomis_flat_df.sort_values(['lsoa11cd'], axis=0, inplace=True)

    # map on other values
    #areas
    area_df = pd.read_csv(os.path.join(os.getcwd(), 'inputs', 'LSOA_area.csv'), skipinitialspace=True)

    area_input_dict = dict(zip(area_df.LSOA11CD,
                               area_df.AREAKM
                               ))

    lacd_input_dict = dict(zip(area_df.LSOA11CD,
                               area_df.LAD11CD
                               ))

    lanm_input_dict = dict(zip(area_df.LSOA11CD,
                               area_df.LAD11NM
                               ))

    nomis_flat_df['area'] = nomis_flat_df['lsoa11cd'].map(area_input_dict)
    nomis_flat_df['lad11cd'] = nomis_flat_df['lsoa11cd'].map(lacd_input_dict)
    nomis_flat_df['lad11nm'] = nomis_flat_df['lsoa11cd'].map(lanm_input_dict)

    # and lat and longs
    ll_df = pd.read_csv(os.path.join(os.getcwd(), 'inputs', 'LSOA_L&L.csv'), skipinitialspace=True)

    lat_input_dict = dict(zip(ll_df.LSOA11CD,
                              ll_df.LATITUDE
                               ))

    long_input_dict = dict(zip(ll_df.LSOA11CD,
                               ll_df.LONGITUDE
                               ))

    nomis_flat_df['lat'] = nomis_flat_df['lsoa11cd'].map(lat_input_dict)
    nomis_flat_df['long'] = nomis_flat_df['lsoa11cd'].map(long_input_dict)

    hh_series = nomis_flat_df.groupby(['lsoa11cd'])['value'].sum()
    hh_dict = hh_series.to_dict()
    nomis_flat_df['hh totals'] = nomis_flat_df['lsoa11cd'].map(hh_dict)

    nomis_flat_df['area'] = nomis_flat_df['area']*(nomis_flat_df['value']/nomis_flat_df['hh totals'])
    nomis_flat_df = nomis_flat_df[['lad11cd', 'lad11nm', 'long', 'lat', 'lsoa11cd', 'lsoa11nm', 'value', 'variable', 'area']]

    nomis_flat_df.to_csv('lsoa_nomis_flat.csv')


def generate_multirun(input_JSON, input_csv, output_JSON, CO_num = 12):
    """config function used to split each enumeration district, as defined in input, into separate runs. Takes a JSON
    file as a template and csv input file (of format below) with info on the enumeration districts - which have been
    built on the assumption the workload should be approximately even.

    CCA 	LA	       LSOA	   number	hh_type 	area
     1	E09000001	E01000001	120	     1	    0.0177808219
     1	E09000001	E01000001	60	     5	    0.008890411

    In the below code cca_makeup details the mixture of households that are present in the cca by LA, LSOA and type.

    """

    # this will hold the new JSON file data
    output_data = defaultdict(dict)

    # open a JSON file to use as template
    with open(input_JSON) as data_file:
        input_data = json.load(data_file)

    # template (a dict) in this case is the level under the top level key
    run_template = input_data['1']
    # district level template
    district_template = run_template['districts']['1']
    # delete the current district in the template
    del run_template["districts"]['1']

    # read in the csv cca data
    with open(input_csv, 'r') as f:
        reader = csv.reader(f)
        next(reader)

        cca_data = list(reader)

    # get a unique list of cca's in the input data
    cca_unique = set()
    for row in cca_data:
        cca_unique.add(row[0])

    # may be better here to convert to a list of named tuples then can refer to keys by name rather than row[x] etc...
    # could also add a shortcut to shorten some of the code below as " output_data[row[0]]['districts'][row[0]]" is used
    # repeatably

    # then run through each row adding a new run for each unique CCA
    for row in cca_data:

        # if the cca is not yet in the output data add it
        if not row[0] in output_data:
            output_data[row[0]] = copy.deepcopy(run_template)
            # add a new district
            output_data[row[0]]['districts'][row[0]] = copy.deepcopy(district_template)
            # set area to zero
            output_data[row[0]]['districts'][row[0]]['district_area'] = 0

        # now add the hh for the current row increasing the area as well
        hh_key = row[4]
        # if cca_makeup not defined add
        if not 'cca_makeup' in output_data[row[0]]['districts'][row[0]]['households'][hh_key]:
            # hh type not in cca so add
            output_data[row[0]]['districts'][row[0]]['households'][hh_key]['cca_makeup'] = defaultdict(dict)

        # then add the actual data
        output_data[row[0]]['districts'][row[0]]['households'][hh_key]['cca_makeup'][row[1]][row[2]] = row[3]
        output_data[row[0]]['districts'][row[0]]['district_area'] += float(row[5])
        output_data[row[0]]['districts'][row[0]]['households'][hh_key]['number'] += int(row[3])

    # for each of the new districts add some CO...currently a fixed number but could vary by area if needed
    list_of_runs = sorted(list(output_data.keys()), key=str)
    for run in list_of_runs:
        output_data[run]['districts'][run]['census officer']['standard']['number'] = CO_num

    # output a JSON file
    with open(os.path.join(output_JSON), 'w') as outfile:
            json.dump(output_data, outfile, indent=4, sort_keys=True)


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


def next_nearest_lsoa(input_lsoa, lookup_table, drop_list):
    """ finds next nearest lsoa not including itself. Requires:

    input_lsoa - an lsoa code
    lookup table - matrix containing straight line distances between codes
    drop_list - a list of lsoa codes that have been inputted already
    """

    # add current lsoa to drop list
    if input_lsoa not in drop_list:
        drop_list.append(input_lsoa)
    # filter lookup table to subset needed
    lookup_table = os.path.join(lookup_table, input_lsoa + '.csv')
    lookup_table_current = pd.read_csv(lookup_table)

    lookup_table_current = lookup_table_current[~lookup_table_current['lsoa11cd'].isin(drop_list)].set_index('lsoa11cd')

    # find the min value in the column returning the row number and then index
    lookup_table_current = lookup_table_current[lookup_table_current[input_lsoa] == lookup_table_current[input_lsoa].min()]

    next_lsoa = lookup_table_current.index.format()[0]

    # add next lsoa to drop list
    if next_lsoa not in drop_list:
        drop_list.append(next_lsoa)

    return next_lsoa


def sum_dict(input_dict):
    """returns the sum of households across La and LSOA in a passed dict"""

    hh_sum = 0
    for LA in input_dict:
        for LSOA in input_dict[LA]:
            hh_sum += int(input_dict[LA][LSOA])

    return hh_sum


def generate_cca_json(input_JSON, input_path, output_path,  hh_per_co=[]):
    # used to create a JSON file showing numbers of each type of hh in each CCA in each LA/LSOA

    # open a JSON template file
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

        #
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
    """calculates dependant on a given ratio of households to census officers the number of CO required and the
    number of households left over if over the set limit of CO.

    cca_output -  a list containing the number of households to add to the next CCA
    cca - the cca id
    la_code -
    lsoa_code -
    hh_remaining - the number of households
    htc - the type id
    area - area hte household cover in km2
    input_ratio - the number of households per CO
    current_co - the number of co currently required to cover the households in the area
    """

    current_co += hh_remaining / input_ratio[htc-1]

    if current_co < 12:
        # return what to put in cca
        return [[cca, la_code, lsoa_code, hh_remaining, htc, area], current_co]
    else:
        # add proportion to current cca and the rest to the next
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


def create_cca_data(input_path, output_path, lookup_table, input_ratios=(), test=False, test_filter =()):
    """creates a cca household level summary ready for conversion to JSON format. The input file must include the
    information below:

    lad11cd - required to link households to la for later aggregation
    lsoa11cd - required to link households to lsoa for later aggregation
    number - the number of households
    hh_type - of a given type
    area - the area the households cover (an average)

    the lookup table is the path to the folder where lookups are stored

    if test is true and filter passed create new set of distance files for included lsoas
    """

    if test:
        # get file llist from lookups folder
        # then for each in filter load to df
        # fitler to include only those in filter
        # and save to test folder
        # then update lookup path tot this new folder
        glob_folder = os.path.join(lookup_table, '*.csv')
        file_list = glob.glob(glob_folder)  # get a list of all files in the folder

        filter_list = list(pd.read_csv(test_filter, header=-1)[0])

        dist_output_path = os.path.join(os.getcwd(), 'raw_inputs', 'test_data', 'test_distances')

        for file_path in file_list:
            file_name = ntpath.basename(file_path)[:-4]
            if file_name in filter_list:
                df = pd.read_csv(file_path)
                df.columns = ('lsoa11cd', file_name)
                df = df[df['lsoa11cd'].isin(filter_list)].set_index('lsoa11cd')
                temp_output_path = os.path.join(dist_output_path, file_name + '.csv')
                df.to_csv(temp_output_path)

        lookup_table = os.path.join(os.getcwd(), 'raw_inputs', 'test_data', 'test_distances')


    start_time = dt.datetime.now()
    print('Started at: ', start_time)

    # load the raw data
    raw_data = pd.read_csv(input_path)  # may be best to read in fields as specified by user - will make row refs neater!

    # initialise variables
    cca = 1  # census collection area
    cca_output = []
    drop_list = []
    current_co = 0
    entries = list(raw_data['lsoa11cd'].unique())  # a list of all the inputted lsoa
    next_lsoa = entries[0]  # start with the first entry
    orig_lsoa_code = next_lsoa

    # for each lsoa
    for i in range(0, len(entries)):

        # subset the input data to only include the current lsoa and remove from the raw data
        subset_list = raw_data[raw_data['lsoa11cd'] == next_lsoa]
        raw_data = raw_data[raw_data['lsoa11cd'] != next_lsoa]

        temp_cca = cca

        # for each row of that subset (lsoa) calculated CO required
        for index, row in subset_list.iterrows():

            la_code = row[0]
            lsoa_code = row[4]
            hh = int(row[6])
            htc = int(row[7])
            area = float(row[8])

            # adding new cca as required until lsos households fully allocated
            hh_to_add = remainder_over(cca_output, cca, la_code, lsoa_code, hh, htc, area, input_ratios, current_co)
            cca_output.append(hh_to_add[0])
            current_co = hh_to_add[1]
            cca = hh_to_add[0][0]

        # if moved on to new cca find next nearest lsoa based on lsoa spilt
        if hh_to_add[0][0] > temp_cca and len(raw_data) > 0:
            next_lsoa = next_nearest_lsoa(lsoa_code, lookup_table, drop_list)
            orig_lsoa_code = next_lsoa
        # else find next based on original
        elif len(raw_data) > 0:
            # same cca so use current lsoa to measure dist
            next_lsoa = next_nearest_lsoa(orig_lsoa_code, lookup_table, drop_list)

        if i > 0 and i % 1000 == 0:
            time_now = dt.datetime.now()
            time_left = ((time_now - start_time).seconds / (i / len(entries))) - (time_now - start_time).seconds
            finish_time = time_now + dt.timedelta(seconds=time_left)
            print('Entry ', i, 'reached. Projected finish time is: ', finish_time)

    # write output to csv
    with open(output_path, "w") as f:
        # add a header
        writer = csv.writer(f)
        writer.writerow(["CCA", "LA", "LSOA", "number", "htc", "area"])
        writer.writerows(cca_output)


# below sets input and output paths for creation of CCA csv summary

ratios = [650]*15  # this is the number of households per CO - same for now but likely to be different
input_csv_path = os.path.join(os.getcwd(), 'raw_inputs', 'test_data', 'test_lsoa_nomis_flat.csv')
output_csv_path = os.path.join(os.getcwd(), 'raw_inputs', 'test_data', 'test_lsoa_cca_nomis.csv')
lookup_csv = os.path.join(os.getcwd(), 'raw_inputs', 'lsoa_distances')
test_filter = os.path.join(os.getcwd(), 'raw_inputs', 'test_data', 'test_filter.csv')
create_cca_data(input_csv_path, output_csv_path, lookup_csv, ratios, test=True, test_filter=test_filter)

# below set input and output paths for creation of JSON file from CSV summary
input_JSON_template = os.path.join(os.getcwd(), 'inputs', 'template_new.JSON')  # JSON template to use
simple_input_path = os.path.join(os.getcwd(), 'raw_inputs', 'lsoa_cca_nomis.csv')
output_JSON_path = os.path.join(os.getcwd(), 'inputs', 'lsoa_nomis.JSON')
#generate_cca_json(input_JSON_template, simple_input_path, output_JSON_path, ratios)

generate_multirun(input_JSON_template, simple_input_path, output_JSON_path)

#generate_nomis_cca()






