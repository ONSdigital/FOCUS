"""Main file for control of FOCUS"""
import random
import initialize
import csv
from itertools import groupby
import simpy
import datetime
from collections import namedtuple
import json
import sys
import os.path
import logging

replication = namedtuple('Replication', ['type', 'run', 'rep', 'time', 'seed'])

LOG_FILENAME = '/home/bigdata/Desktop/nas/projects/FOCUS/outputs/error.txt'
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG,)

file_name = input('Enter input file name: ')
if len(file_name) < 1:
    file_name = 'default single census 2011.JSON'

# loads the selected config file
try:
    with open(file_name) as data_file:
        input_data = json.load(data_file)  # dict of the whole file

except IOError as e:
    print(e)
    sys.exit()

list_of_runs = sorted(list(input_data.keys()), key=int)  # returns top level of config file

###########################################
# for simple and I hope temp output only
names = ['run', 'rep', 'number', 'type', 'area', 'allow paper', 'paper_after_max_visits', 'FU on', 'default resp', 'paper prop', 'FU start time', 'dig assist eff',
         'dig assist flex', 'max visits', 'contact rates', 'call conversion rate', 'conversion rate', 'enumerators', 'advisers', 'letter sent',
         'letter effect', 'responses', 'dig resp', 'paper resp', 'total visits', 'unn visits', 'wasted visits',
         'visit out', 'visit success', 'visit contact', 'visit assist', 'visit paper', 'calls', 'phone responses', 'letter wasted',
         'letter received', 'letter responses', 'seed']

raw_output = input('Enter output file name: ')
if len(raw_output) < 1:
    raw_output = 'simple_test.csv'

try:
    output_file_path = os.getcwd() + '/outputs/' + raw_output
    if os.path.isfile(output_file_path) is True:
        print("file", raw_output, 'was overwritten')
    else:
        print("file", raw_output, 'was created')

    with open('outputs/' + raw_output, 'w', newline='') as csv_file:
        output_file = csv.writer(csv_file, delimiter=',')
        output_file.writerow(names)

except IOError as e:
    print(e)
    sys.exit()

#################################################

for run in list_of_runs:

    try:

        output_data = []

        sim_start = datetime.datetime.strptime(input_data[run]['start_date'], '%Y, %m, %d, %H, %M, %S')
        sim_end = datetime.datetime.strptime(input_data[run]['end_date'], '%Y, %m, %d, %H, %M, %S')
        sim_hours = (sim_end - sim_start).total_seconds()/3600

        replications = input_data[run]['replications']

        for rep in range(replications):

            now = datetime.datetime.now()
            seed_date = datetime.datetime(2012, 4, 12, 19, 00, 00)
            seed = abs(now - seed_date).total_seconds() + int(run)
            rnd = random.Random()
            rnd.seed(seed)

            env = simpy.Environment()
            output_data.append(replication('start', int(run), rep + 1, now, seed))

            current_run = initialize.Run(env, input_data[run], output_data, raw_output, rnd, run, rep + 1, seed,
                                         LOG_FILENAME)

            env.run(until=sim_hours)

            now = datetime.datetime.now()
            output_data.append(replication('end', int(run), rep + 1, now, seed))
            rep += 1

        '''Event outputs'''
        output_data.sort(key=lambda x: type(x).__name__)

        for k, g in groupby(output_data, lambda x: type(x).__name__):
            with open('outputs/{}.csv'.format(k), 'a', newline='') as f_output:  # use a for append
                csv_output = csv.writer(f_output)
                rows = list(g)
                csv_output.writerow(list(rows[0]._fields))
                for row in rows:
                    csv_output.writerow(list(row))

    except (StopIteration, KeyError, AttributeError):
        logging.exception('Exception in run {0}, with seed {1}'.format(run, seed))
