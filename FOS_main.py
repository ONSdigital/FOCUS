"""Main control file"""
import json
import datetime as dt
import random
import simpy
import initialise
import os
import shutil
import post_process as pp
import copy
from collections import defaultdict
from itertools import repeat
from multiprocessing import cpu_count, Pool, freeze_support, Lock
import helper as hp
import pandas as pd
import output_options as oo
import glob
import gzip
import logging
import traceback

import matplotlib.pyplot as plt
plt.style.use('ggplot')

l = Lock()


def start_run(run_input, seeds, out_path):

    max_output_file_size = 100000000
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

    # generate list of codes for reference from input file where appropriate
    dig_list = ['False', 'True']
    la_list = []
    lsoa_list = []
    hh_type_list = []

    # cycle through the input data to only include those household types, lsoa and la that exist to appear in the output
    for d_key, d_value in run_input['districts'].items():
        for hh_key, hh_value in d_value['households'].items():
            if hh_key not in hh_type_list:
                hh_type_list.append(hh_key)
            for la_key, la_value in hh_value['cca_makeup'].items():
                if la_key not in la_list:
                    la_list.append(la_key)
                for lsoa_key, lsoa_value in la_value.items():
                    if lsoa_key not in lsoa_list:
                        lsoa_list.append(lsoa_key)

    """
    passive_summary records a daily summary of returns that occur without any intervention
    passive_totals records a simple total of returns without intervention

    active_summary records all returns including those due to the interventions
    active_totals records a simple total of all returns

    active_paper_summary paper returns over time
    active_paper_totals total paper returns

    visit_summary records all visits over time by the level in the key
    visit_totals records simple sum of total visits by level in key

    time_summary records time spent on task by the census officers
    time_totals sum of total time on task

    paper_summary summary of paper given out over time
    paper_totals sum of paper given out

    the summaries and totals are at the levels in the keys.
    """

    passive_summary = {}
    passive_totals = {}
    active_summary = {}
    active_totals = {}
    active_paper_summary = {}
    active_paper_totals = {}
    visit_totals = {}
    visit_summary = {}
    time_totals = {}
    time_summary = {}
    paper_totals = {}
    paper_summary = {}

    if oo.record_passive_summary:

        passive_summary = {'la': dict((la_list[i], [0]*days) for i in range(0, len(la_list))),
                           'lsoa': dict((lsoa_list[i], [0] * days) for i in range(0, len(lsoa_list))),
                           'digital': dict((dig_list[i], [0]*days) for i in range(0, len(dig_list))),
                           'hh_type': dict((hh_type_list[i], [0]*days) for i in range(0, len(hh_type_list)))
                           }

        passive_totals = {'lsoa': dict((lsoa_list[i], 0) for i in range(0, len(lsoa_list))),
                          'la': dict((la_list[i], 0) for i in range(0, len(la_list)))
                          }

    if oo.record_active_summary:

        active_summary = {'la': dict((la_list[i], [0] * days) for i in range(0, len(la_list))),
                          'digital': dict((dig_list[i], [0] * days) for i in range(0, len(dig_list))),
                          'hh_type': dict((hh_type_list[i], [0] * days) for i in range(0, len(hh_type_list)))
                          }

        active_totals = {'lsoa': dict((lsoa_list[i], 0) for i in range(0, len(lsoa_list))),
                         'la': dict((la_list[i], 0) for i in range(0, len(la_list)))
                         }

    if oo.record_active_paper_summary:

        active_paper_summary = {'la': dict((la_list[i], [0] * days) for i in range(0, len(la_list))),
                                'hh_type': dict((hh_type_list[i], [0] * days) for i in range(0, len(hh_type_list)))
                                }

        active_paper_totals = {'lsoa': dict((lsoa_list[i], 0) for i in range(0, len(lsoa_list))),
                               'la': dict((la_list[i], 0) for i in range(0, len(la_list)))
                               }

    if oo.record_visit_summary:

        visit_totals = {'la': dict((la_list[i], 0) for i in range(0, len(la_list)))
                        }

        visit_summary = {'la': dict((la_list[i], [0]*days) for i in range(0, len(la_list))),
                         'hh_type': dict((hh_type_list[i], [0] * days) for i in range(0, len(hh_type_list)))}

    if oo.record_time_summary:

        time_totals = {'la': dict((la_list[i], 0) for i in range(0, len(la_list)))}

        time_summary = {'la': dict((la_list[i], [0] * days) for i in range(0, len(la_list))),
                        'lsoa': dict((lsoa_list[i], [0] * days) for i in range(0, len(lsoa_list)))}

    if oo.record_paper_summary:

        paper_totals = {'la': dict((la_list[i], 0) for i in range(0, len(la_list)))}

        paper_summary = {'la': dict((la_list[i], [0] * days) for i in range(0, len(la_list))),
                         'hh_type': dict((hh_type_list[i], [0] * days) for i in range(0, len(hh_type_list)))}

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
                   passive_totals,
                   active_summary,
                   active_totals,
                   active_paper_summary,
                   active_paper_totals,
                   visit_summary,
                   visit_totals,
                   time_summary,
                   time_totals,
                   paper_summary,
                   paper_totals,
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

    if oo.record_passive_summary:

        hp.output_summary(summary_path, passive_summary, 'passive_summary', c_run, c_rep)
        hp.output_summary(summary_path, passive_totals, 'passive_totals', c_run, c_rep)

    if oo.record_active_summary:

        hp.output_summary(summary_path, active_summary, 'active_summary', c_run, c_rep)
        hp.output_summary(summary_path, active_totals, 'active_totals', c_run, c_rep)

    if oo.record_active_paper_summary:
        hp.output_summary(summary_path, active_paper_summary, 'active_paper_summary', c_run, c_rep)
        hp.output_summary(summary_path, active_paper_totals, 'active_paper_totals', c_run, c_rep)

    if oo.record_visit_summary:

        hp.output_summary(summary_path, visit_summary, 'visit_summary', c_run, c_rep)
        hp.output_summary(summary_path, visit_totals, 'visit_totals', c_run, c_rep)

    if oo.record_time_summary:

        hp.output_summary(summary_path, time_summary, 'time_summary', c_run, c_rep)
        hp.output_summary(summary_path, time_totals, 'time_totals', c_run, c_rep)

    if oo.record_paper_summary:

        hp.output_summary(summary_path, paper_summary, 'paper_summary', c_run, c_rep)
        hp.output_summary(summary_path, paper_totals, 'paper_totals', c_run, c_rep)


def produce_default_output(input_path):
    """Produces default charts and outputs if turned on. If not leaves the raw data untouched."""

    try:
        print('Rationalising log files')
        # combine output files
        current_path = os.path.join(input_path, 'summary')
        folders = next(os.walk(current_path))[1]
        folders = [os.path.join(current_path, folder) for folder in folders]

        pp_pool = Pool(cpu_count())
        pp_pool.starmap(pp.produce_rep_results, zip(folders))
        pp_pool.close()
        pp_pool.join()

    except:

        print('failed in post processing')

    # produce a plot of the active and passive using bokeh
    print('Producing response plots')
    active_response_path = os.path.join(input_path, 'summary', 'active_summary', 'digital', 'average.csv')
    passive_response_path = os.path.join(input_path, 'summary', 'passive_summary', 'digital', 'average.csv')
    output_path = os.path.join(input_path, 'summary')
    pp.bokeh_line_chart(passive_response_path, active_response_path, 'passive', 'active', output_path, 'response.html',
                        cumulative=False)

    summary_outpath = os.path.join(input_path, 'summary')
    pandas_data = pp.csv_to_pandas(input_path, ['hh_record'])

    print('Producing summary tables')

    visits_path = os.path.join(input_path, 'summary', 'visit_summary', 'la')
    pp.plot_summary(visits_path, summary_outpath, 'visits', reps=False, cumulative=True, individual=False)

    visits_path = os.path.join(input_path, 'summary', 'time_summary', 'la')
    pp.plot_summary(visits_path, summary_outpath, 'time', reps=False, cumulative=True, individual=False)

    paper_path = os.path.join(input_path, 'summary', 'paper_summary', 'la')
    pp.plot_summary(paper_path, summary_outpath, 'paper', reps=False, cumulative=False, individual=False)

    # pyramid chart showing comparison of two strategies on LSOA return rates (or passive and active if one sim)
    print("Producing lsoa level summary")

    input_left = pd.read_csv(os.path.join(input_path, 'summary', 'passive_totals', 'lsoa', 'average.csv'), index_col=0)
    name_left = 'Passive'
    input_right = pd.read_csv(os.path.join(input_path, 'summary', 'active_totals', 'lsoa', 'average.csv'), index_col=0)
    name_right = 'Active'

    pp.sum_pyramid(pandas_data['hh_record'], summary_outpath, input_left, input_right, name_left, name_right, bin_size=2)


if __name__ == '__main__':

    create_new_config = False
    produce_default = True
    multiple_processors = True  # set to false to debug
    delete_old = False
    freeze_support()

    # delete all old output files but not the main directory.
    if delete_old:

        if os.path.isdir('outputs/'):
            dirs = os.listdir(os.path.join(os.getcwd(), 'outputs'))
            for direc in dirs:
                delete_path = os.path.join(os.getcwd(), 'outputs', direc)
                if os.path.isfile(delete_path):
                    os.unlink(delete_path)
                else:
                    shutil.rmtree(delete_path)

    first_cwd = os.getcwd()
    input_path = os.path.join(os.getcwd(), 'inputs')
    # get list of files to run here rather than loading them in
    glob_folder = os.path.join(input_path, '*.JSON')
    simulation_list = glob.glob(glob_folder)  # get a list of all files(sim runs) in the folder

    outputs = 'outputs'
    output_path = os.path.join(os.getcwd(), outputs)

    sims = len(simulation_list)
    sim_counter = 1
    # for each sim file in the input folder
    for sim in simulation_list:

        print("Simulation ", sim_counter, " of ", len(simulation_list),  " started.")

        try:

            os.chdir(first_cwd)  # reset to overall working directory each time
            sim_name = os.path.basename(sim)[:-5]  # get filename without the .JSON extension
            sub_path_name = sim_name + " " + str(dt.datetime.now().strftime("%Y""-""%m""-""%d %H.%M.%S"))
            output_JSON_name = sim_name + " " + str(dt.datetime.now().strftime("%Y""-""%m""-""%d %H.%M.%S")) + '.JSON'
            current_output_path = os.path.join(output_path, sub_path_name)
            if not os.path.isdir(current_output_path):
                os.makedirs(current_output_path)

            with open(sim) as data_file:
                input_data = json.load(data_file)

            # get list of all districts in simulation
            list_of_districts = sorted(list(input_data.keys()), key=int)
            districts = max([int(district) for district in list_of_districts])
            reps = input_data['1']['replications']  # extract replications
            max_runs = reps * districts

            # define a list to be used to map all run/replication combinations to available processors
            run_list = []
            seed_dict = {}
            seed_list = []
            dict_all = defaultdict()

            st = dt.datetime.now()
            print('Simulation started at: ', st)
            counter = 0

            # place, with random seeds, a copy of the run/rep into the run list
            for district in list_of_districts:
                input_data[district]['run_id'] = district
                seed_dict[str(district)] = {}
                for rep in range(1, input_data[district]['replications'] + 1):

                    if str(rep) not in input_data[district]['replication seeds']:
                        now = dt.datetime.now()
                        seed_date = dt.datetime(2012, 4, 12, 19, 00, 00)
                        seed = abs(now - seed_date).total_seconds() + int(district) + rep
                        #seed = 10  # uncomment to use same random seed for debug
                        seed_dict[str(input_data[district]['run_id'])][str(rep)] = seed
                        if not str(input_data[district]['run_id']) in dict_all:
                            dict_all[str(input_data[district]['run_id'])] = {}
                        dict_all[str(input_data[district]['run_id'])][str(rep)] = seed
                        create_new_config = True

                    else:
                        seed = input_data[district]['replication seeds'][str(rep)]

                    seed_list.append(seed)

                    input_data[district]['rep_id'] = rep
                    run_list.append(copy.deepcopy(input_data[district]))
                    counter += 1

                    # if run list len is chunk size run them...
                    if len(run_list) == cpu_count()*4 or (district == str(districts) and rep == reps):

                        # different run methods - use single processor for debugging
                        if multiple_processors:
                            pool = Pool(cpu_count())
                            pool.starmap(start_run,
                                         zip(run_list, seed_list, repeat(current_output_path)))
                            pool.close()
                            pool.join()
                        else:
                            for i in range(len(run_list)):
                                start_run(run_list[i], seed_list[i], current_output_path)

                        # calculate finish time for current simulation and print progress
                        time_now = dt.datetime.now()
                        time_left = ((time_now - st).seconds / (counter / max_runs)) - (time_now - st).seconds
                        finish_time = time_now + dt.timedelta(seconds=time_left)

                        print(pp.roundup((counter / max_runs) * 100, 1), "percent of current simulation complete. "
                                                                         "Projected finish time is:", finish_time)
                        run_list = []
                        seed_dict[str(district)] = {}
                        seed_list = []

            # add back in the ability to output a record, with seeds, as to what has been run...
            # dict all contains the required seeds

            if create_new_config:
                print("Saving config file")

                json_file_path = os.path.join(output_path, output_JSON_name)

                list_of_seed_runs = sorted(list(dict_all.keys()), key=int)
                # first assign the seeds...
                for run in list_of_seed_runs:
                    input_data[run]['replication seeds'] = dict_all[run]
                    # delete ids
                    del input_data[run]['run_id']
                    del input_data[run]['rep_id']

                with open(json_file_path, 'w') as outfile:
                    json.dump(input_data, outfile)

                with open(json_file_path, 'rb') as f_in:
                    with gzip.open(json_file_path + '.gz', 'w') as f_out:
                        shutil.copyfileobj(f_in, f_out)

                # delete orig JSON file
                os.remove(json_file_path)

                """to read a gzip file
                with gzip.open('file.txt.gz', 'rb') as f:
                    file_content = f.read()
                """

            print('Simulation complete at time: ', dt.datetime.now())

            if produce_default:
                print("Starting post processing.")
                produce_default_output(current_output_path)

        except Exception as e:

            logging.error(traceback.format_exc())
            print('Simulation error encountered in simulation: ', sim)

        cet = dt.datetime.now()
        print(sim_counter, 'of', sims, 'complete at time', cet)
        sim_counter += 1

    # overall end time
    et = dt.datetime.now()
    print('All simulations complete at time: ', et)
