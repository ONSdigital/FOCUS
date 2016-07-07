"""Main control file"""
import sys
import json
import datetime
import random
import simpy
import initialisev2
import os
import csv
import shutil
import post_process
import time
import copy
from collections import defaultdict
from itertools import repeat
from multiprocessing import cpu_count, Pool, freeze_support, Lock

l = Lock()  # global declaration...can I avoid this?


def start_run(run_input, seeds, out_path):

    # pull out length of sim for current run
    sim_start = datetime.datetime.strptime(run_input['start_date'], '%Y, %m, %d, %H, %M, %S')
    sim_end = datetime.datetime.strptime(run_input['end_date'], '%Y, %m, %d, %H, %M, %S')
    sim_hours = (sim_end - sim_start).total_seconds()/3600

    output_data = defaultdict(list)
    rnd = random.Random()
    rnd.seed(str(seeds))

    # define simpy env for current rep
    env = simpy.Environment()

    # initialise replication
    initialisev2.Rep(env, run_input, output_data, rnd, run_input['id'], sim_hours, run_input['rep id'])

    # and run it
    env.run(until=sim_hours)

    # write the output to csv files
    list_of_output = sorted(list(output_data.keys()))

    l.acquire()

    for row in list_of_output:
        if os.path.isdir(out_path + '/{}'.format(row) + '/') is False:
            os.mkdir(out_path + '/{}'.format(row) + '/')
        with open(out_path + '/{}'.format(row) + '/' + str(run_input['id']) + '.csv', 'a', newline='') as f_output:
            csv_output = csv.writer(f_output)
            for data_row in output_data[row]:
                rows = list(data_row)
                csv_output.writerow(list(rows))

    l.release()


def produce_default_output():

    agrregated_data = post_process.aggregate(output_path)

    post_process.create_response_map(output_path, agrregated_data, 'inputs/geog_E+W_LAs.geojson')
    #post_process.create_response_map(output_path, agrregated_data, 'inputs/geog_E+W_LAs.geojson', 'paper', True, True)
    #post_process.create_response_map(output_path, agrregated_data, 'inputs/geog_E+W_LAs.geojson', 'digital', True, False)
    #post_process.create_visit_map(output_path, agrregated_data, 'inputs/geog_E+W_LAs.geojson', "Visit_paper", True, True)


if __name__ == '__main__':

    create_new_config = False
    produce_default = True
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
        file_name = 'inputs/LA_hh.JSON'
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

    pool = Pool(cpu_count())
    Pool().starmap(start_run, zip(run_list, seed_list, repeat(output_path)))

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
