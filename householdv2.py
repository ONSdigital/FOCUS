"""file used to store the classes and definitions related to the households represented in the simulation """

import datetime
from collections import namedtuple

response_times = namedtuple('Responded', ['reps', 'District', 'hh_id', 'Type', 'Time'])  # time full response received


class Household(object):

    # Create an instance of the class
    def __init__(self, rep, rnd, env, district, hh_id, hh_type, input_data, output_data):

        self.rep = rep
        self.rnd = rnd
        self.env = env
        self.district = district
        self.hh_id = hh_id
        self.hh_type = hh_type
        self.input_data = input_data
        self.output_data = output_data


        self.resp_level = self.input_data['default_resp']
        #self.help_level = self.input_data['default_help']
        self.help_level = 0

        self.status = ''

        self.rep.env.process(self.action())

    def action(self):

        action_test = self.rnd.uniform(0, 100)  # represents the COA to be taken.

        if action_test <= self.resp_level:

            current_date_time = self.rep.start_date + datetime.timedelta(hours=self.rep.env.now)
            sim_days_left = (self.rep.end_date.date() - current_date_time.date()).days

            days_until_response = beta_dist(self.rep,
                                            self.input_data['beta_dist'][0],
                                            self.input_data['beta_dist'][1],
                                            sim_days_left)

            response_date = current_date_time.date() + datetime.timedelta(days=days_until_response)

            response_day = response_date.weekday()
            response_time = gauss_dist(self.rnd,
                                       self.input_data['response_time'][str(response_day)][0],
                                       self.input_data['response_time'][str(response_day)][1])

            response_date_time = datetime.datetime.combine(response_date, datetime.datetime.min.time())\
                                + datetime.timedelta(hours=response_time)

            response_date_time = (response_date_time - current_date_time).total_seconds()/3600

            yield self.env.timeout(response_date_time)
            self.output_data.append(response_times(self.rep.reps,
                                                   self.district.name,
                                                   self.hh_id,
                                                   self.hh_type,
                                                   self.env.now))

        elif self.resp_level < action_test <= self.help_level:

            print('call')
            contact_time = 1  # all call at same time for testing really some dist...from call centre patterns
            self.status = "making contact"
            yield self.env.timeout(contact_time)
            yield self.env.process(self.contact())

        else:

            pass
            # yield until end of sim
            #print('nothing')

    def contact(self):

        #print(str(self.hh_type) + "  called at " + str(self.env.now))

        current_ad = yield self.rep.adviser_store.get()
        #print(str(current_ad) + " is with " + str(self.hh_type) + " at time " + str(self.env.now))
        yield self.env.timeout(0.5)
        self.rep.adviser_store.put(current_ad)


# beta dist used to generate some numbers for responses over time
def beta_dist(rep, alpha, beta, sim_days_left):
    # return (rep.rnd.betavariate(alpha, beta))*(rep.sim_hours - rep.env.now)
    return int((rep.rnd.betavariate(alpha, beta))*sim_days_left)


def gauss_dist(rnd, alpha, beta):

    response_time = rnd.gauss(alpha, beta)

    if response_time < 0:
        response_time = 0



    return response_time







