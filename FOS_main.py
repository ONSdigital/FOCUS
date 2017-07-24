"""Main control file"""
import sys
import json
import datetime as dt
import random
import simpy
import initialise
import os
import shutil
import post_process as pp
import time
import copy
from collections import defaultdict
from itertools import repeat
from multiprocessing import cpu_count, Pool, freeze_support, Lock
import helper as hp
import pandas as pd
import output_options as oo

import matplotlib.pyplot as plt
plt.style.use('ggplot')

l = Lock()


def start_run(run_input, seeds, max_runs, out_path):

    max_output_file_size = 1000000000
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
    for day in range(0, days):
        day_cols.append(day)

    # generate list of codes for reference from raw inputs
    la_list = hp.generate_list(os.path.join(os.getcwd(), 'raw_inputs', 'la lookup.csv'), 0)
    lsoa_list = hp.generate_list(os.path.join(os.getcwd(), 'raw_inputs', 'lsoa lookup.csv'), 0)
    district_list = [str(district) for district in range(1, max_runs)]
    dig_list = ['0', '1']
    hh_type_list = [str(hh_type) for hh_type in range(1, 16)]

    passive_summary = {'la': dict((la_list[i], [0]*days) for i in range(0, len(la_list))),
                       'digital': dict((dig_list[i], [0]*days) for i in range(0, len(dig_list))),
                       'hh_type': dict((hh_type_list[i], [0]*days) for i in range(0, len(hh_type_list))),
                       'district_name': dict((district_list[i], [0]*days) for i in range(0, len(district_list)))}

    active_summary = {'la': dict((la_list[i], [0]*days) for i in range(0, len(la_list))),
                      'digital': dict((dig_list[i], [0] * days) for i in range(0, len(dig_list))),
                      'hh_type': dict((hh_type_list[i], [0] * days) for i in range(0, len(hh_type_list))),
                      'district_name': dict((district_list[i], [0]*days) for i in range(0, len(district_list)))}

    active_totals = {'lsoa': dict((lsoa_list[i], 0) for i in range(0, len(lsoa_list))),
                     'la': dict((la_list[i], 0) for i in range(0, len(la_list))),
                     'district_name': dict((district_list[i], 0) for i in range(0, len(district_list)))}

    passive_totals = {'lsoa': dict((lsoa_list[i], 0) for i in range(0, len(lsoa_list))),
                      'la': dict((la_list[i], 0) for i in range(0, len(la_list))),
                      'district_name': dict((district_list[i], 0) for i in range(0, len(district_list)))}

    l.acquire()
    if oo.record_key_info:
        if not os.path.isdir(os.path.join(out_path, 'key info')):
            os.mkdir(os.path.join(out_path, 'key info'))

        if not os.path.isfile(os.path.join(out_path, 'key dates', run_input['run_id'] + ".csv")):
            pd.DataFrame(temp_list).to_csv(os.path.join(out_path, 'key info', run_input['run_id'] + ".csv"))
        else:
            pd.DataFrame(temp_list).to_csv(os.path.join(out_path, 'key info', run_input['run_id'] + ".csv"), mode='a',
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
                   passive_summary,
                   active_summary,
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
    hp.write_output(output_data, out_path, run_input['run_id'])

    # write summary data to csv to defined folder in main outputs
    summary_path = os.path.join(out_path, 'summary')

    c_run = run_input['run_id']
    c_rep = run_input['rep_id']
    l.acquire()
    hp.output_summary(summary_path, passive_summary, 'passive_summary', c_run, c_rep)
    hp.output_summary(summary_path, passive_totals, 'passive_totals', c_run, c_rep)
    hp.output_summary(summary_path, active_summary, 'active_summary', c_run, c_rep)
    hp.output_summary(summary_path, active_totals, 'active_totals', c_run, c_rep)
    l.release()
    with open('counter.csv', 'r') as fle:
        counter = int(fle.readline()) + 1

    if counter % 100 == 0:
        print(counter, " cca out of ", max_runs, " complete")

    with open('counter.csv', 'w') as fle:
        fle.write(str(counter))


def produce_default_output(current_path):
    """Produces default charts and outputs if turned on. If not leaves the raw data untouched."""

    # line chart of overall responses over time
    pp.produce_rep_results(current_path)
    default_path = os.path.join(current_path, 'summary', 'active_summary', 'digital')
    pp.plot_summary(default_path, reps=False, cumulative=True, individual=True)

    # pyramid chart showing comparison of two strategies on LSOA return rates
    pandas_data = pp.csv_to_pandas(output_path, ['hh_record'])
    input_left = pd.read_csv(os.path.join(current_path, 'summary', 'passive_totals', 'lsoa', 'average.csv'), index_col=0)
    name_left = 'Passive'
    input_right = pd.read_csv(os.path.join(current_path, 'summary', 'active_totals', 'lsoa', 'average.csv'), index_col=0)
    name_right = 'Active'

    pp.sum_pyramid(pandas_data['hh_record'], input_left, input_right, name_left, name_right)


if __name__ == '__main__':

    create_new_config = False
    produce_default = True
    multiple_processors = True  # set to false to debug
    delete_old = False
    freeze_support()

    # counter to track progression of run
    with open('counter.csv', 'w') as fle:
        fle.write(str(0))

    # delete all old output files but not the directories.
    if delete_old:
        if os.path.isdir('outputs/'):
            dirs = os.listdir(os.path.join(os.getcwd(), 'outputs'))
            for d in dirs:
                if d != 'outputs/':
                    shutil.rmtree(os.path.join(os.getcwd(), 'outputs', d))

    # read in input configuration file using a default if nothing is selected
    input_path = input('Enter input file path or press enter to use defaults: ')
    if len(input_path) < 1:
        file_name = 'inputs/subset_lsoa_nomis.JSON'
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
        input_data[run]['run_id'] = run
        seed_dict[str(run)] = {}
        for rep in range(1, input_data[run]['replications'] + 1):

            if str(rep) not in input_data[run]['replication seeds']:
                now = dt.datetime.now()
                seed_date = dt.datetime(2012, 4, 12, 19, 00, 00)
                seed = abs(now - seed_date).total_seconds() + int(run) + rep
                seed_dict[str(input_data[run]['run_id'])][str(rep)] = seed
                create_new_config = True

            else:
                seed = input_data[run]['replication seeds'][str(rep)]

            seed_list.append(seed)

            input_data[run]['rep_id'] = rep
            run_list.append(copy.deepcopy(input_data[run]))

    ts = time.time()
    st = dt.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    output_JSON_name = str(dt.datetime.now().strftime("%Y""-""%m""-""%d %H.%M.%S")) + '.JSON'
    print(st)

    max_runs = max([int(run) for run in list_of_runs]) + 1

    # different run methods - use single processor for debugging
    if multiple_processors:
        pool = Pool(cpu_count())  # use the next two lines to use multiple processors
        Pool().starmap(start_run, zip(run_list, seed_list, repeat(max_runs), repeat(output_path)))
    else:
        for i in range(len(run_list)):
            start_run(run_list[i], seed_list[i], max_runs, output_path)

    # at the end add the seed list and print out the JSON?
    if create_new_config:

        list_of_seed_runs = sorted(list(seed_dict.keys()), key=int)
        # first assign the seeds...
        for run in list_of_seed_runs:
            input_data[run]['replication seeds'] = seed_dict[run]
            # delete ids
            del input_data[run]['run_id']
            del input_data[run]['rep_id']

        with open(os.path.join(output_path, output_JSON_name), 'w') as outfile:
            json.dump(input_data, outfile)

    print('Simulation complete at time: ', dt.datetime.now())

    if produce_default:
        produce_default_output(output_path)

    ts = time.time()
    st = dt.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    print(st)
