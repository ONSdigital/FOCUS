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
from collections import defaultdict
from multiprocessing import cpu_count, Pool
import time


ts = time.time()

st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
print(st)

# set required flags
create_new_config = []
data_lists = {}


def start_run(run_input):

    output_path = os.path.join(os.getcwd(), 'outputs')

    # pull out length of sim for current run
    sim_start = datetime.datetime.strptime(run_input['start_date'], '%Y, %m, %d, %H, %M, %S')
    sim_end = datetime.datetime.strptime(run_input['end_date'], '%Y, %m, %d, %H, %M, %S')
    sim_hours = (sim_end - sim_start).total_seconds()/3600

    # number of replications to run
    replications = run_input['replications']

   # run each replication
    for rep in range(replications):

        output_data = defaultdict(list)

        # set a random seed based on current date and current rep unless seed exists
        if str(rep) not in run_input['replication seeds']:
            now = datetime.datetime.now()
            seed_date = datetime.datetime(2012, 4, 12, 19, 00, 00)
            seed = abs(now - seed_date).total_seconds() + int(run_input['id'])
            run_input['replication seeds'][rep] = seed  # added...

        else:
            seed = run_input['replication seeds'][str(rep)]

        rnd = random.Random()
        rnd.seed(str(seed))

        # define simpy env for current rep
        env = simpy.Environment()

        # initialise replication
        initialisev2.Rep(env, run_input, output_data, rnd, run_input['id'], sim_hours, rep + 1, seed)

        # and run it
        env.run(until=sim_hours)

        # write the output to csv files
        list_of_output = sorted(list(output_data.keys()))

        for row in list_of_output:
            if os.path.isdir(output_path + '/{}'.format(row) + '/') is False:
                os.mkdir(output_path + '/{}'.format(row) + '/')
            with open(output_path + '/{}'.format(row) + '/' + str(run_input['id']) + '.csv', 'a', newline='') as f_output:
                csv_output = csv.writer(f_output)
                for data_row in output_data[row]:
                    rows = list(data_row)
                    csv_output.writerow(list(rows))

if __name__ == '__main__':

    # delete all old output files from default location except generated JSON files
    if os.path.isdir('outputs/') is True:
        dirs = [x[0] for x in os.walk('outputs/')]
        for d in dirs:
            if d != 'outputs/':
                shutil.rmtree(str(d))

    # read in input configuration file - use a default if nothing is selected
    input_path = input('Enter input file path or press enter to use defaults: ')
    if len(input_path) < 1:
        file_name = 'inputs/small_test_LA_hh.JSON'
        input_path = os.path.join(os.getcwd(), file_name)

    # loads the selected config file
    try:
        with open(input_path) as data_file:
            input_data = json.load(data_file)  # dict of the whole file

    # if something goes wrong exit with error
    except IOError as e:
        print(e)
        sys.exit()

    # ask for output destination but still need to check if can create new folders
    output_path = input('Enter output path or press enter to use default: ')
    if len(output_path) < 1:
        outputs = 'outputs'
        output_path = os.path.join(os.getcwd(), outputs)

    try:
        # create if does not exist
        if os.path.isdir(output_path) is False:
            os.makedirs(output_path)

    # if something goes wrong exit with error
    except IOError as e:
        print(e)
        sys.exit()

    # create list of runs from config file
    list_of_runs = sorted(list(input_data.keys()), key=int)  # returns top level of config file
    the_list = []

    counter = 1

    for item in list_of_runs:
        input_data[item]['id'] = counter
        the_list.append(input_data[item])
        counter += 1

    Pool().map(start_run, the_list)

    output_JSON_name = str(datetime.datetime.now().strftime("%Y""-""%m""-""%d %H.%M.%S")) + '.JSON'
    with open(os.path.join(output_path, output_JSON_name), 'w') as outfile:
        json.dump(input_data, outfile)

    ts = time.time()

    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    print(st)




