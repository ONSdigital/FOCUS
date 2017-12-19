"""module that creates daily return profiles based a simple csv input file as well as allowing for these
profiles to be sampled"""


import pandas as pd
import os


def response_profiles_2011_all(census_day):
    """takes a file of the format:

    Date  Day  hh_type1  hh_type2  hh_type3

               Daily returns by hh_type here

    and generates a profile to use for sampling within the simulation. Result is dataframe in the same format
    but in cumulative distribution and with a generic catch all profile added.
    """
    # generates a profile to use for response rates based on 2011 data

    input_path = os.path.join(os.getcwd(), 'raw_inputs', 'summary_returns_2017.csv')
    responses = pd.read_csv(input_path)
    # make all headers lowercase as they are used as keys
    responses.columns = map(str.lower, responses.columns)
    # don't need these columns
    responses.drop(['date', 'day'], axis=1, inplace=True)
    responses_only = responses

    # do a reduction to better represent self response in 2011
    #total_days = len(responses_only)
    #for i in range(total_days):
    #    if i > census_day:
    #        reduction = (i - census_day)/(total_days-census_day)
    #        responses_only.iloc[i] *= (1-reduction)

    responses_only['generic'] = responses_only.sum(axis=1)

    # convert to a probability distribution
    prob_response = responses_only.divide(responses_only.sum(axis=0), axis=1)
    #prob_response.to_csv('dist_data.csv')

    # create cumulative probability
    cum_prob_response = prob_response.cumsum(axis=0)

    # return a dataframe of the cumulative probability of response
    return cum_prob_response


def sample_day_2011_all(rep, hh_type, generic=True):

    if generic:
        # use first col
        rt = rep.response_df["generic"]

    else:
        # use col specific to hh type
        rt = rep.response_df[hh_type]

    r = rep.rnd.uniform(0, 1)

    return min([index for index, value in enumerate(rt) if value > r])










