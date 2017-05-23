"""module containing functions used across modules"""
import datetime as dt
import ntpath
import math
import os
import csv
from multiprocessing import Lock
from sys import getsizeof
import sys
import response_profiles
import call_profiles as cp
import numpy as np
import pandas as pd

l = Lock()  # global declaration...can I avoid this


def responses_to_date(district, output_format=""):
    # need to add on those households who returned before the first interaction - and were therefore never added
    # to the sim

    count = len([household.hh_id for household in district.households if household.responded])

    if output_format == "%":

        return (count + district.early_responders)/(len(district.households) + district.early_responders)

    else:
        return ((count + district.early_responders)/(len(district.households) + district.early_responders))*100


def current_day(obj):
    # return the day of the week based upon days elasped from start of sim

    return (obj.rep.start_day + math.floor(obj.env.now / 24) % 7) % 7


def str_to_dec(str_time):
    # test added

    times = str_time.split(":")

    return float(times[0]) + float(times[1])/60


def make_time_decimal(time_object):
    # test added

    hours = time_object.hour
    mins = time_object.minute
    secs = time_object.second

    return hours + mins/60 + secs/3600


def return_time_key(input_dict, time):

    time %= 24

    key_list = sorted(list(input_dict.keys()), key=int)

    for key in key_list:
        if int(key) >= time:
            return key


def str2bool(value):
    # test added
    return str(value).lower() in ("True", "true", "1")


def beta_dist(rep, alpha, beta, sim_days_left):
    # return (rep.rnd.betavariate(alpha, beta))*(rep.sim_hours - rep.env.now)
    return int((rep.rnd.betavariate(alpha, beta))*sim_days_left)


def write_output(output_data, out_path, ed_id):
    # write the output to csv files
    list_of_output = sorted(list(output_data.keys()))
    l.acquire()

    for row in list_of_output:
        if not os.path.isdir(out_path + '/{}'.format(row) + '/'):
            os.mkdir(out_path + '/{}'.format(row) + '/')
        # test here if file exists, in no create headers if yes don't
        if not os.path.isfile(out_path + '/{}'.format(row) + '/' + str(ed_id) + '.csv'):
            with open(out_path + '/{}'.format(row) + '/' + str(ed_id) + '.csv', 'a', newline='') as f_output:
                csv_output = csv.writer(f_output)
                csv_output.writerow(list(output_data[row][0]._fields))

        with open(out_path + '/{}'.format(row) + '/' + str(ed_id) + '.csv', 'a', newline='') as f_output:
            csv_output = csv.writer(f_output)
            rows = []
            for data_row in output_data[row]:
                rows.append(data_row)
                #rows = list(data_row)

            csv_output.writerows(rows)

    # clear output file
    output_data.clear()
    l.release()


def write_summary(df, event_time, event_index):
    """updates the passed df"""

    day = math.floor(event_time / 24)

    if event_index not in df.index:
        df.loc[event_index] = 0

    # the below is quite slow - but fastest for df. Would a numpy array be quicker if possible.
    df.at[event_index, day] += 1


def dict_size(a_dict):
    # returns memory size of dictionary

    size = 0

    for key, value in a_dict.items():

            size += getsizeof(value)

    return size


def set_preference(paper_prop, rnd):
    """sets whether the hh prefers paper or digital"""
    paper_test = rnd.uniform(0, 100)

    if paper_test <= int(paper_prop):

        return 0

    return 1


def set_behaviour(digital, input_data, behaviour, rnd):

    if digital or input_data["paper_allowed"]:
        # use default
        len_beh = len(input_data['behaviours']['default'][behaviour])
        return input_data['behaviours']['default'][behaviour][min(len_beh - 1)]
    else:
        len_beh = len(input_data['behaviours']['alt'][behaviour])
        return input_data['behaviours']['alt'][behaviour][min(len_beh - 1)]


def set_household_response_time(rep, input_data, hh_type, digital):

    # returns the day a response is sent based upon specified distribution
    response_day = response_profiles.sample_day_2011_all(rep, hh_type)

    # returns the time of day the response is received - again final number and type of dists to use to be determined.
    if response_day == rep.census_day:
        # use census day profile
        day_response_time = rep.rnd.gauss(input_data['response_time']['census_day'][0],
                                          input_data['response_time']['census_day'][1])
    else:
        # use some other profile
        day_response_time = rep.rnd.gauss(input_data['response_time']['other'][0],
                                          input_data['response_time']['other'][1])

    final_response_time = (response_day*24) + day_response_time

    # comment out if do not need to check sampled data is correct
    # with open('sampled_data.csv', 'a') as fp:
    #     writer = csv.writer(fp)
    #     writer.writerow([final_response_time])

    #    fp.close()

    return final_response_time


def set_household_call_time(rep):
    # returns the day and time a call is made based upon specified distribution
    call_day = cp.sample_calls_2011_all(rep)

    dow = (rep.start_day + call_day % 7) % 7

    if call_day == rep.census_day:

        day = 'census day'

    elif dow < 6:

        day = 'weekday'

    elif dow == 6:

        day = 'saturday'

    else:

        day = 'sunday'

    day_call_time = cp.sample_calls_day_2011(rep, day)

    final_call_time = ((call_day - 1) * 24) + day_call_time

    return final_call_time


def get_entity_time(entity, type="start"):

    date_type = type + "_date"
    index = 0
    if type == "end":
        index = -1

    try:
        # check date has valid availability schedule
        date = dt.date(*map(int, entity.input_data[date_type].split(',')))
        # convert date to sim (simpy) time
        date_sim = (date - entity.rep.start_date).total_seconds() / 3600

        # convert time of that day to sim time
        time = entity.input_data['availability'][str(date.weekday())][index]
        time_sim = make_time_decimal(dt.time(*map(int, time.split(':'))))

        return date_sim + time_sim

    except IndexError as e:

        if not entity.rep.districts:
            # if no districts must be an error that applies to whole run (advisers)
            print(e, "Run ", entity.rep.run, " has no availability schedule set for adviser on: ",
                  entity.input_data[date_type])

        else:

            print(e, "District ", entity.district.name, " has no availability schedule set for CO on start day of ",
                  entity.input_data[date_type])

            sys.exit()


def get_event_time(event):

    # check date has valid availability schedule
    date = dt.date(*map(int, event.input_data["start_date"].split(',')))
    # convert date to sim (simpy) time
    date_sim = (date - event.rep.start_date).total_seconds() / 3600

    # convert time of that day to sim time
    time = event.input_data["time"]

    return date_sim + time


def get_time_of_return(input_time, rep):
    # temp function to used to select a time of day for returns after reminders
    # assumes delivery at 9

    input_day = math.floor(input_time / 24)
    dow = (rep.start_day + input_day % 7) % 7

    if input_day == rep.census_day:

        day = 'census day'

    elif dow < 6:

        day = 'weekday'

    elif dow == 6:

        day = 'saturday'

    else:

        day = 'sunday'

    rt = rep.call_day_df[day].as_matrix()
    rt = rt[18:]
    r = rep.rnd.uniform(0, 1)
    time = (np.argwhere(rt == min(rt[(rt - r) > 0]))[0][0])/2 + rep.rnd.uniform(0, 0.5)
    return time

