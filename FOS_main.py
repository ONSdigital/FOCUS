"""Main control file"""
import sys
import json
import datetime as dt
import random
import simpy
import initialise
import os
import shutil
import post_process
import time
import copy
from collections import defaultdict
from itertools import repeat
from multiprocessing import cpu_count, Pool, freeze_support, Lock
import helper as hp
import pandas as pd
import glob
import output_options as oo

l = Lock()


def start_run(run_input, seeds, out_path):

    max_output_file_size = 1000000000
    # pull out length of sim for current run
    start_date = dt.date(*map(int, run_input['start_date'].split(',')))
    end_date = dt.date(*map(int, run_input['end_date'].split(',')))
    sim_hours = (end_date - start_date).total_seconds()/3600
    census_date = dt.date(*map(int, run_input['census_date'].split(',')))
    census_day = (census_date - start_date).days

    # write key dates/info to a csv for later use in post processing
    temp_list = [{'start_date': start_date,
                  'end_date': end_date,
                  'census_date': census_date,
                  'census_day': census_day,
                  'sim_hours': sim_hours}]

    # create dataframe to hold summary data
    days = int(sim_hours / 24)+1
    day_cols = []
    for i in range(0, days):
        day_cols.append(i)

    # generate list of codes from raw inputs
    la_list = hp.generate_list(os.path.join(os.getcwd(), 'raw_inputs', 'la lookup.csv'), 0)
    lsoa_list = hp.generate_list(os.path.join(os.getcwd(), 'raw_inputs', 'lsoa lookup.csv'), 0)
    #district_list = hp.generate_list(os.path.join(os.getcwd(), 'raw_inputs', 'CCA_all.csv'), 0)
    district_list = [str(i) for i in range(1, 1949)]

    dig_list = ['0', '1']

    # this list needs to come from a raw data source when we know which hh to include
    hh_type_list = [str(i) for i in range(1, 16)]

    # a dataframe used to store passive stats
    passive_data_summary = {'la': dict((la_list[i], [0]*days) for i in range(0, len(la_list))),
                            'digital': dict((dig_list[i], [0]*days) for i in range(0, len(dig_list))),
                            'hh_type': dict((hh_type_list[i], [0]*days) for i in range(0, len(hh_type_list))),
                            'district_name': dict((district_list[i], [0]*days) for i in range(0, len(district_list)))}

   #passive_data_summary = {}

    # a dict that contains dataframes used to store the active summary stats as runs progress
    active_data_summary = {'la': dict((la_list[i], [0]*days) for i in range(0, len(la_list))),
                           'digital': dict((dig_list[i], [0] * days) for i in range(0, len(dig_list))),
                           'hh_type': dict((hh_type_list[i], [0] * days) for i in range(0, len(hh_type_list))),
                           'district_name': dict((district_list[i], [0]*days) for i in range(0, len(district_list)))}

    #active_data_summary = {}

    # used to count the responses to date in ....
    active_totals = {'lsoa': dict((lsoa_list[i], 0) for i in range(0, len(lsoa_list))),
                     'la': dict((la_list[i], 0) for i in range(0, len(la_list))),
                     'district_name': dict((district_list[i], 0) for i in range(0, len(district_list)))}



    # a count of the households in each ...
    passive_totals = {'lsoa': dict((lsoa_list[i], 0) for i in range(0, len(lsoa_list))),
                      'la': dict((la_list[i], 0) for i in range(0, len(la_list))),
                      'district_name': dict((district_list[i], 0) for i in range(0, len(district_list)))}

    #passive_totals = {}

    l.acquire()
    if oo.record_key_info:
        if not os.path.isdir(os.path.join(out_path, 'key info')):
            os.mkdir(os.path.join(out_path, 'key info'))

        if not os.path.isfile(os.path.join(out_path, 'key dates', run_input['run id'] + ".csv")):
            pd.DataFrame(temp_list).to_csv(os.path.join(out_path, 'key info', run_input['run id'] + ".csv"))
        else:
            pd.DataFrame(temp_list).to_csv(os.path.join(out_path, 'key info', run_input['run id'] + ".csv"), mode='a',
                                           header=False)

    l.release()

    output_data = defaultdict(list)

    rnd = random.Random()
    rnd.seed(str(seeds))

    # define simpy env for current rep
    env = simpy.Environment()

    # initialise replication
    initialise.Rep(env,
                   run_input,
                   output_data,
                   passive_data_summary,
                   active_data_summary,
                   active_totals,
                   passive_totals,
                   rnd,
                   sim_hours,
                   start_date,
                   census_day,
                   out_path,
                   max_output_file_size)

    # and run it
    env.run(until=sim_hours)

    # write the output to csv files
    hp.write_output(output_data, out_path, run_input['run id'])

    # write summary data to csv to defined folders
    # check if folder exists - if not create and output to csv
    for k, v in passive_data_summary.items():

        # create a folder if one doesn't already exist
        temp_output_path = os.path.join(os.getcwd(), 'charts', 'passive summary', k)
        if not os.path.isdir(temp_output_path):
            os.makedirs(temp_output_path)

        pd.DataFrame.from_dict(v, orient='columns', dtype=None).T.\
            to_csv(os.path.join(os.getcwd(), 'charts', 'passive summary', k, run_input['run id'] + '.csv'))

    for k, v in passive_totals.items():

        # create a folder if one doesn't already exist
        temp_output_path = os.path.join(os.getcwd(), 'charts', 'passive summary totals', k)
        if not os.path.isdir(temp_output_path):
            os.makedirs(temp_output_path)

        pd.DataFrame.from_dict(v, orient='index', dtype=None). \
            to_csv(os.path.join(os.getcwd(), 'charts', 'passive summary totals', k, run_input['run id'] + '.csv'), header=False)

    for k, v in active_data_summary.items():

        # create a folder if one doesn't already exist
        temp_output_path = os.path.join(os.getcwd(), 'charts', 'active summary', k)
        if not os.path.isdir(temp_output_path):
            os.makedirs(temp_output_path)

        pd.DataFrame.from_dict(v, orient='columns', dtype=None).T. \
            to_csv(os.path.join(os.getcwd(), 'charts', 'active summary', k, run_input['run id'] + '.csv'))

    for k, v in active_totals.items():

        # create a folder if one doesn't already exist
        temp_output_path = os.path.join(os.getcwd(), 'charts', 'active summary totals', k)
        if not os.path.isdir(temp_output_path):
            os.makedirs(temp_output_path)

        pd.DataFrame.from_dict(v, orient='index', dtype=None). \
            to_csv(os.path.join(os.getcwd(), 'charts', 'active summary totals', k, run_input['run id'] + '.csv'))


def produce_default_output():
    # this produces some default processed data for run 1 only in some cases...
    # defaults to LA level to produce outputs that fit into the Data Vis map format

    # select data to read into data frame structure
    pandas_data = post_process.csv_to_pandas(output_path, ['Return_sent', 'hh_record', 'Responded', 'key info'])

    # produce summary stats for data vis map - default is by LA
    post_process.returns_summary(pandas_data['hh_record'], pandas_data['Responded'])
    post_process.returns_summary(pandas_data['hh_record'], pandas_data['Responded'], resp_type='paper')
    post_process.returns_summary(pandas_data['hh_record'], pandas_data['Responded'], resp_type='digital')

    # do we always want to select this data frame - yes for the default output
    glob_folder = os.path.join('outputs', 'hh_record', '*.csv')
    file_list = glob.glob(glob_folder)  # get a list of all files in the folder

    default_key = str(file_list[0].split(os.path.sep)[-1])[:-4]
    df1 = pandas_data['Return_sent'][default_key]
    df2 = pandas_data['hh_record'][default_key]
    start_date = pandas_data['key info'][default_key].start_date[0]
    start_date = dt.date(*map(int, start_date.split('-')))

    # produce return chart over time
    post_process.produce_return_charts(df1, df2, 'Active', 'Passive', start_date, ' returns run 1.html')

    # example of how to produce a second chart based on next run - can also be used as second strategy in waterfall
    # df3 = pandas_data['Return_sent']['2']
    # df4 = pandas_data['hh_record']['2']
    # post_process.produce_return_charts(df3, df4, '  returns run 2.html')

    # produce comparison of final results
    post_process.pyramid([df2, df2, 'passive', True], [df1, df2, 'active', False], bins=[65, 105, 5])


if __name__ == '__main__':

    create_new_config = False
    produce_default = False
    multiple_processors = True
    delete_old = True
    freeze_support()

    # delete all old output files from default location on new run of sim if tag is True.
    if delete_old:
        if os.path.isdir('outputs/'):
            dirs = os.listdir(os.path.join(os.getcwd(), 'outputs'))
            for d in dirs:
                if d != 'outputs/':
                    shutil.rmtree(os.path.join(os.getcwd(), 'outputs', d))

    # read in input configuration file using a default if nothing is selected
    input_path = input('Enter input file path or press enter to use defaults: ')
    if len(input_path) < 1:
        file_name = 'inputs/lsoa_nomis.JSON'
        input_path = os.path.join(os.getcwd(), file_name)

    try:
        with open(input_path) as data_file:
            input_data = json.load(data_file)

    # if something goes wrong exit with error
    except IOError as e:
        print(e)
        sys.exit()

    # ask for output destination or use a default if none specified
    output_path = input('Enter output path or press enter to use default: ')
    if len(output_path) < 1:
        outputs = 'outputs'
        sub_path_name = str(dt.datetime.now().strftime("%Y""-""%m""-""%d %H.%M.%S"))
        output_path = os.path.join(os.getcwd(), outputs, sub_path_name)

    try:
        if not os.path.isdir(output_path):
            os.makedirs(output_path)

    # if something goes wrong exit with error
    except IOError as e:
        print(e)
        sys.exit()

    # create a list of runs from configuration file
    list_of_runs = sorted(list(input_data.keys()), key=int)
    # define a list to be used to map all run/replication combinations to available processors
    run_list = []
    seed_dict = {}
    seed_list = []

    # place, with random seeds, a copy of the run/rep into the run list
    for run in list_of_runs:
        input_data[run]['run id'] = run
        seed_dict[str(run)] = {}
        for rep in range(1, input_data[run]['replications'] + 1):

            if str(rep) not in input_data[run]['replication seeds']:
                now = dt.datetime.now()
                seed_date = dt.datetime(2012, 4, 12, 19, 00, 00)
                seed = abs(now - seed_date).total_seconds() + int(run) + rep
                seed_dict[str(input_data[run]['run id'])][str(rep)] = seed
                create_new_config = True

            else:
                seed = input_data[run]['replication seeds'][str(rep)]

            seed_list.append(seed)

            input_data[run]['rep id'] = rep
            run_list.append(copy.deepcopy(input_data[run]))

    ts = time.time()
    st = dt.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    output_JSON_name = str(dt.datetime.now().strftime("%Y""-""%m""-""%d %H.%M.%S")) + '.JSON'
    print(st)

    # different run methods - use single processor for debugging
    if multiple_processors:
        pool = Pool(cpu_count())  # use the next two lines to use multiple processors
        Pool().starmap(start_run, zip(run_list, seed_list, repeat(output_path)))
    else:
        for i in range(len(run_list)):
            start_run(run_list[i], seed_list[i], output_path)

    # at the end add the seed list and print out the JSON?
    if create_new_config:

        list_of_seed_runs = sorted(list(seed_dict.keys()), key=int)
        # first assign the seeds...
        for run in list_of_seed_runs:
            input_data[run]['replication seeds'] = seed_dict[run]
            # delete ids
            del input_data[run]['run id']
            del input_data[run]['rep id']

        with open(os.path.join(output_path, output_JSON_name), 'w') as outfile:
            json.dump(input_data, outfile)

    if produce_default:
        produce_default_output()

    ts = time.time()
    st = dt.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    print(st)
