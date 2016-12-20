"""Main control file"""
import sys
import json
import datetime
import random
import simpy
import initialisev2
import os
import shutil
import post_process
import time
import copy
from collections import defaultdict
from multiprocessing import cpu_count, Pool, freeze_support, Lock
from helper import write_output


l = Lock()  # global declaration...can I avoid this?


def start_run(run_input, seeds, out_path):

    # pull out length of sim for current run
    sim_start = datetime.datetime.strptime(run_input['start_date'], '%Y, %m, %d, %H, %M, %S')
    sim_end = datetime.datetime.strptime(run_input['end_date'], '%Y, %m, %d, %H, %M, %S')
    sim_hours = (sim_end - sim_start).total_seconds()/3600
    census_date = datetime.datetime.strptime(run_input['census_date'], '%Y, %m, %d, %H, %M, %S')
    census_day = ((census_date - sim_start).total_seconds()/86400)+1

    output_data = defaultdict(list)

    rnd = random.Random()
    rnd.seed(str(seeds))

    # define simpy env for current rep

    env = simpy.Environment()

    # initialise replication
    initialisev2.Rep(env,
                     run_input,
                     output_data,
                     rnd,
                     run_input['id'],
                     sim_hours,
                     census_day,
                     run_input['rep id'],
                     out_path)

    # and run it
    env.run(until=sim_hours)

    # write the output to csv files

    write_output(output_data, out_path, run_input['id'])


def produce_default_output():

    # select data to read into data frames
    pandas_data = post_process.csv_to_pandas(output_path, ['Returned', 'Visit', 'hh_count', 'Visit_contact'])

    try:
        # produce cumulative summary of overall returns
        cumulative_returns = post_process.cumulative_sum(pandas_data['Returned']['1'], 0, 1440, 24, 'district')
        hh_count = pandas_data['hh_count']['1']
        hh_count.index = cumulative_returns.index
        returns_summary = cumulative_returns.div(hh_count['hh_count'], axis='index')
        returns_summary.to_csv(os.path.join(output_path, "Returns summary.csv"))

        # also need an E+W average for each
        overall_returns = cumulative_returns.sum(axis=0)
        # get hh_totals
        hh_totals = pandas_data['hh_count']['1']['hh_count'].sum()
        average_returns = (overall_returns / hh_totals) * 100
        print("E&W average response rates")
        print(average_returns.tolist())

        # produce summary of digital returns
        cumulative_dig_returns = post_process.cumulative_sum(pandas_data['Returned']['1'], 0, 1440, 24, 'district', 'digital')
        hh_count.index = cumulative_dig_returns.index
        dig_returns_summary = cumulative_dig_returns.div(hh_count['hh_count'], axis='index')
        dig_returns_summary.to_csv(os.path.join(output_path, "Digital returns summary.csv"))

        overall_dig_returns = cumulative_dig_returns.sum(axis=0)
        average_dig_returns = (overall_dig_returns / hh_totals)*100
        print("E&W average digital response rates")
        print(average_dig_returns.tolist())

        # produce summary of paper returns
        cumulative_pap_returns = post_process.cumulative_sum(pandas_data['Returned']['1'], 0, 1440, 24, 'district', 'paper')
        hh_count.index = cumulative_pap_returns.index
        pap_returns_summary = cumulative_pap_returns.div(hh_count['hh_count'], axis='index')
        pap_returns_summary.to_csv(os.path.join(output_path, "Paper returns summary.csv"))

        overall_pap_returns = cumulative_pap_returns.sum(axis=0)
        average_pap_returns = (overall_pap_returns / hh_totals)*100
        print("E&W average paper response rates")
        print(average_pap_returns.tolist())

    # if something goes wrong exit with error
    except TypeError as e:
        print("There is no input named Returned")

    try:

        # visits summary as proportion of total number of visits
        cumulative_visits = post_process.cumulative_sum(pandas_data['Visit']['1'], 0, 1440, 24, 'district')
        # divide by the max visits for each district
        visit_summary = cumulative_visits.divide(cumulative_visits.max(axis=1), axis=0)
        visit_summary.to_csv(os.path.join(output_path, "visit summary.csv"))

        overall_visits = cumulative_visits.sum(axis=0)
        average_visits = (overall_visits / overall_visits.max(axis=0)) * 100
        print("E&W average visits")
        print(average_visits.tolist())

    except TypeError as e:
        print("There is no input named Visit")

    # proportion of visits where contact was made
    # proportion of wasted visits over time
    # chart for resource utilisation over time by htc
    # response rates over time by htc


if __name__ == '__main__':

    create_new_config = False
    produce_default = False
    freeze_support()

    # delete all old output files from default location except generated JSON files.
    if os.path.isdir('outputs/'):
        dirs = [x[0] for x in os.walk('outputs/')]
        for d in dirs:
            if d != 'outputs/':
                shutil.rmtree(str(d))

    # read in input configuration file using a default if nothing is selected
    input_path = input('Enter input file path or press enter to use defaults: ')
    if len(input_path) < 1:
        #file_name = 'inputs/single multi district.JSON'
        file_name = 'inputs/management areas.JSON'
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
        output_path = os.path.join(os.getcwd(), outputs)

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
        input_data[run]['id'] = run
        seed_dict[str(run)] = {}
        for rep in range(1, input_data[run]['replications'] + 1):

            if str(rep) not in input_data[run]['replication seeds']:
                now = datetime.datetime.now()
                seed_date = datetime.datetime(2012, 4, 12, 19, 00, 00)
                seed = abs(now - seed_date).total_seconds() + int(run) + rep
                seed_dict[str(input_data[run]['id'])][str(rep)] = seed
                create_new_config = True

            else:
                seed = input_data[run]['replication seeds'][str(rep)]

            seed_list.append(seed)

            input_data[run]['rep id'] = rep
            run_list.append(copy.deepcopy(input_data[run]))

    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    print(st)

    #pool = Pool(cpu_count())
    #Pool().starmap(start_run, zip(run_list, seed_list, repeat(output_path)))
    start_run(run_list[0], seed_list[0], output_path)  # uncomment this for a single run without multi processing

    # at the end add the seed list and print out the JSON?
    if create_new_config:

        list_of_seed_runs = sorted(list(seed_dict.keys()), key=int)
        # first assign the seeds...
        for run in list_of_seed_runs:
            input_data[run]['replication seeds'] = seed_dict[run]
            # delete ids
            del input_data[run]['id']
            del input_data[run]['rep id']

        output_JSON_name = str(datetime.datetime.now().strftime("%Y""-""%m""-""%d %H.%M.%S")) + '.JSON'
        with open(os.path.join(output_path, output_JSON_name), 'w') as outfile:
            json.dump(input_data, outfile)

    if produce_default:
        produce_default_output()

    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    print(st)
