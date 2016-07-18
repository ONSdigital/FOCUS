"""module containing functions used across modules"""
import datetime as dt
import ntpath
import math


def roundup_nearest_ten(x):
    return int(math.ceil(x / 10.0)) * 10


def rounddown_nearest_ten(x):
    return int(math.floor(x / 10.0)) * 10


def returns_to_date(district, output_format=""):

    count = len([hh.id for hh in district.households if hh.returned])

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

    return dt.datetime.strptime(time, '%H,%M,%S').time()


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
    # determine date and time of response
    current_date_time = obj.rep.start_date + dt.timedelta(hours=obj.rep.env.now)
    sim_days_left = (obj.rep.end_date.date() - current_date_time.date()).days

    days_until_response = beta_dist(obj.rep,
                                    obj.input_data['beta_dist'][0],
                                    obj.input_data['beta_dist'][1],
                                    sim_days_left)

    response_date = current_date_time.date() + dt.timedelta(days=days_until_response)

    response_day = response_date.weekday()
    response_time = gauss_dist(obj.rnd,
                               obj.input_data['response_time'][str(response_day)][0],
                               obj.input_data['response_time'][str(response_day)][1])

    response_date_time = dt.datetime.combine(response_date, dt.datetime.min.time()) \
                         + dt.timedelta(hours=response_time)
    # issue here with response time - gauss dist can create -ve time if action is run during day not at start
    response_date_time_hours = max((response_date_time - current_date_time).total_seconds() / 3600, 0)

    return response_date_time_hours


def renege_time(obj):

    return obj.rep.rnd.uniform(obj.input_data['call_renege_lower'], obj.input_data['call_renege_upper'])
