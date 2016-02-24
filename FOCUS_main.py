"""Main file for control of FOCUS"""
import random
import initialize
import csv
from itertools import groupby
import simpy
import datetime
from collections import namedtuple
import json

replication = namedtuple('Replication', ['type', 'run', 'rep', 'time', 'seed'])


# add IO error catching

# allow user to select desired configuration file
file_name = input('Enter file name: ')
if len(file_name) < 1:
    file_name = 'default single.JSON'

# loads the selected config file
with open(file_name) as data_file:
    input_data = json.load(data_file)  # dict of the whole file

list_of_runs = sorted(list(input_data.keys()), key=int)  # returns top level of config file - iterate through this list to do all the runs

names = ['run', 'rep', 'number', 'area', 'allow paper', 'paper_after_max_visits', 'FU on', 'default resp', 'paper prop', 'FU start time', 'dig assist eff',
         'dig assist flex', 'max visits', 'contact rates', 'call conversion rate', 'conversion rate', 'enumerators', 'advisers', 'letter sent',
         'letter effect', 'responses', 'dig resp', 'paper resp', 'total visits', 'unn visits', 'wasted visits',
         'visit out', 'visit success', 'visit contact', 'visit assist', 'visit paper', 'calls', 'phone responses', 'letter wasted',
         'letter received', 'letter responses', 'seed']

# add code to print to a file instead/as well
raw_output = 'RAW_testing9.csv'
with open('outputs/' + raw_output, 'w', newline='') as csv_file:
    output_file = csv.writer(csv_file, delimiter=',')
    output_file.writerow(names)

for run in list_of_runs:

    # this will need to catch all other possible errors that occur...and record to error file
    try:

        output_data = []  # for output...

        sim_start = datetime.datetime.strptime(input_data[run]['start_date'], '%Y, %m, %d, %H, %M, %S')
        sim_end = datetime.datetime.strptime(input_data[run]['end_date'], '%Y, %m, %d, %H, %M, %S')
        sim_hours = (sim_end - sim_start).total_seconds()/3600

        replications = input_data[run]['replications']

        reps = 0

        for reps in range(replications):

            now = datetime.datetime.now()
            seed_date = datetime.datetime(2012, 4, 12, 19, 00, 00)
            seed = abs(now - seed_date).total_seconds() + int(run)
            rnd = random.Random()
            rnd.seed(seed)

            env = simpy.Environment()
            output_data.append(replication('start', int(run), reps + 1, now, seed))
            current_run = initialize.Run(env, input_data[run], output_data, raw_output, rnd, run, reps + 1, seed)

            env.run(until=sim_hours)

            now = datetime.datetime.now()
            output_data.append(replication('end', int(run), reps + 1, now, seed))
            reps += 1

        #output_data.sort(key=lambda x: type(x).__name__)

        #for k, g in groupby(output_data, lambda x: type(x).__name__):
        #    with open('outputs/{}.csv'.format(k), 'w', newline='') as f_output:  # use a for append
        #        csv_output = csv.writer(f_output)
        #        rows = list(g)
        #        csv_output.writerow(list(rows[0]._fields))
        #        for row in rows:
        #            csv_output.writerow(list(row))
    except:
        # skip runs that cause errors but...
        # add code to give more detail in an error log at some stage as to why!
        # and print to a file...
        print('Run:', run, 'failed with random seed', seed)





