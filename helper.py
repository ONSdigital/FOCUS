"""module containing functions used across modules"""
import datetime as dt
import ntpath
import math
import os
import csv
from multiprocessing import Lock
from sys import getsizeof

l = Lock()  # global declaration...can I avoid this


def roundup_nearest_ten(x):
    return int(math.ceil(x / 10.0)) * 10


def rounddown_nearest_ten(x):
    return int(math.floor(x / 10.0)) * 10


def returns_to_date(district, output_format=""):

    count = len([household.hh_id for household in district.households if household.returned])

    if output_format == "%":

        return count/len(district.households)

    else:
        return (count/len(district.households))*100


def current_day(obj):

    current_date_time = obj.rep.start_date + dt.timedelta(hours=obj.rep.env.now)
    current_date = current_date_time.date()
    day = current_date.weekday()

    return day


def simpy_to_time(simpy_time):

    days = int(simpy_time/24)
    hours = int(simpy_time - days*24)
    mins = ((simpy_time - days*24) - hours)*60
    secs = int((mins - int(mins))*60)

    time = str(hours) + "," + str(int(mins)) + "," + str(secs)

    return dt.datetime.strptime(time, '%H,%M,%S').time()


def make_time(hours, mins, secs):

    time = str(hours) + "," + str(mins) + "," + str(secs)

    #return dt.datetime.strptime(time, '%H,%M,%S').time()
    return dt.time(*map(int, time.split(',')))  # this is much quicker!


def make_time_decimal(time_object):

    hours = time_object.hour
    mins = time_object.minute
    secs = time_object.second

    return hours + mins/60 + secs/3600


def return_time_key(input_dict, time):

    time = make_time_decimal(simpy_to_time(time))

    key_list = sorted(list(input_dict.keys()), key=int)

    for key in key_list:
        if int(key) >= time:
            return key


def str2bool(value):
    return str(value).lower() in ("True", "true", "1")


def beta_dist(rep, alpha, beta, sim_days_left):
    # return (rep.rnd.betavariate(alpha, beta))*(rep.sim_hours - rep.env.now)
    return int((rep.rnd.betavariate(alpha, beta))*sim_days_left)


def gauss_dist(rnd, alpha, beta):

    output = rnd.gauss(alpha, beta)

    if output < 0:
        output = 0

    return output


def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)


def clamp(x):
    return max(0, min(x, 255))


def next_day(simpy_time):
    # round up to next nearest day...
    next = math.ceil(simpy_time/24)*24
    return next - simpy_time + 9


def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + dt.timedelta(n)


def return_resp_time(obj):
    # determine date and time of response -  look to speed this up...
    current_date_time = obj.rep.start_date + dt.timedelta(hours=obj.rep.env.now)
    sim_days_left = (obj.rep.end_date.date() - current_date_time.date()).days

    days_until_response = beta_dist(obj.rep,
                                    obj.input_data['response_day'][0],
                                    obj.input_data['response_day'][1],
                                    sim_days_left)

    response_date = current_date_time.date() + dt.timedelta(days=days_until_response)

    response_day = response_date.weekday()
    response_time = gauss_dist(obj.rnd,
                               obj.input_data['response_time']['other'][0],
                               obj.input_data['response_time']['other'][1])

    response_date_time = dt.datetime.combine(response_date, dt.datetime.min.time()) \
                         + dt.timedelta(hours=response_time)
    # issue here with response time - gauss dist can create -ve time if action is run during day not at start
    response_date_time_hours = max((response_date_time - current_date_time).total_seconds() / 3600, 0)

    return response_date_time_hours


def renege_time(obj):

    return obj.rep.rnd.uniform(obj.input_data['call_renege_lower'], obj.input_data['call_renege_upper'])


def write_output(output_data, out_path, run_input_id):
    # write the output to csv files
    list_of_output = sorted(list(output_data.keys()))
    l.acquire()

    for row in list_of_output:
        if not os.path.isdir(out_path + '/{}'.format(row) + '/'):
            os.mkdir(out_path + '/{}'.format(row) + '/')
        # test here if file exists, in no create headers if yes don't
        if not os.path.isfile(out_path + '/{}'.format(row) + '/' + str(run_input_id) + '.csv'):
            with open(out_path + '/{}'.format(row) + '/' + str(run_input_id) + '.csv', 'a', newline='') as f_output:
                csv_output = csv.writer(f_output)
                csv_output.writerow(list(output_data[row][0]._fields))

        with open(out_path + '/{}'.format(row) + '/' + str(run_input_id) + '.csv', 'a', newline='') as f_output:
            csv_output = csv.writer(f_output)
            for data_row in output_data[row]:
                rows = list(data_row)
                csv_output.writerow(list(rows))

    # clear output file
    output_data.clear()

    l.release()


def dict_size(a_dict):

    size = 0

    for key, value in a_dict.items():

            size += getsizeof(value)

    return size


def set_preference(paper_prop, rnd):
    """sets whether the hh prefers paper or digital and the associated time to receive responses from both"""
    paper_test = rnd.uniform(0, 100)

    if paper_test <= int(paper_prop):

        return False

    return True


def set_behaviour(digital, input_data, behaviour, rnd):

    if digital or input_data["paper_allowed"]:
        # use default
        len_beh = len(input_data['behaviours']['default'][behaviour])
        return input_data['behaviours']['default'][behaviour][min(len_beh - 1)]
    else:
        len_beh = len(input_data['behaviours']['alt'][behaviour])
        return input_data['behaviours']['alt'][behaviour][min(len_beh - 1)]


def set_household_response_time(rep, input_data, sim_hours):

    # returns a day of response from a beta dist - the final dist that will be used is still to be determined.
    raw_response_time = (rep.rnd.betavariate(input_data['response_day'][0],
                                             input_data['response_day'][1]))*sim_hours
    response_day = math.ceil(raw_response_time/24)

    # returns the time of day the response is received - again final number and type of dists to use to be determined.
    if response_day == rep.census_day:
        # use census day profile
        day_response_time = rep.rnd.gauss(input_data['response_time']['census_day'][0],
                                          input_data['response_time']['census_day'][1])
    else:
        # use some other profile
        day_response_time = rep.rnd.gauss(input_data['response_time']['other'][0],
                                          input_data['response_time']['other'][1])

    final_response_time = ((response_day-1)*24) + day_response_time

    return final_response_time
