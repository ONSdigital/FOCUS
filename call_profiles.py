"""module that creates daily call profiles based a simple csv input file as well as allowing for these
profiles to be sampled"""

import numpy as np
import pandas as pd
import os


def call_profiles_2011_all():
    # generates a profile to use for calls to the help line based on 2011 data

    input_path = os.path.join(os.getcwd(), 'raw_inputs', 'call profile.csv')
    calls = pd.read_csv(input_path)
    # make all headers lowercase as they are used as keys
    calls.columns = map(str.lower, calls.columns)
    # don't need these columns
    #########################
    calls_only = calls.drop(['date', 'day'], axis=1)

    # convert to a probability distribution

    prob_calls = calls_only.divide(calls_only.sum(axis=0), axis=1)

    # ans now create cumulative probability
    cum_prob_calls = prob_calls.cumsum(axis=0)

    # return a dataframe of the cumulative probability of response
    return cum_prob_calls


def call_profiles_day_2011():

    input_path = os.path.join(os.getcwd(), 'raw_inputs', 'call_times_in_day.csv')
    call_day = pd.read_csv(input_path)
    # make all headers lowercase as they are used as keys
    call_day.columns = map(str.lower, call_day.columns)

    call_day = call_day.drop(['interval'], axis=1)

    # convert to a probability distribution
    prob_call_day = call_day.divide(call_day.sum(axis=0), axis=1)
    # and now create cumulative probability
    cum_prob_call_day = prob_call_day.cumsum(axis=0)

    # return a dataframe of the cumulative probability of response
    return cum_prob_call_day


def sample_calls_2011_all(rep):
    # returns the position in a matrix where an argument is found
    # in this case the position of the first true element

    rt = rep.call_df.as_matrix()
    r = rep.rnd.uniform(0, 1)
    return np.argwhere(rt == min(rt[(rt - r) > 0]))[0][0] + 1


def sample_calls_day_2011(rep, day_type):

    rt = rep.call_day_df[day_type].as_matrix()
    r = rep.rnd.uniform(0, 1)
    time = (np.argwhere(rt == min(rt[(rt - r) > 0]))[0][0])/2 + rep.rnd.uniform(0, 0.5)
    return time



