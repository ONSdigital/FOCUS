"""file used to store the classes and definitions related to the households represented in the simulation """

import datetime
import censusv2
from simpy.util import start_delayed
from collections import namedtuple

response = namedtuple('Responded', ['reps', 'District', 'hh_id', 'Type', 'Time'])  # time full response received


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

        # below define how the hh behaves
        self.resp_level = self.input_data['default_resp']
        #self.help_level = self.input_data['default_help']
        self.help_level = 0
        self.resp_type = set_preference(self)
        self.delay = self.input_data['delay'][self.resp_type]

        # flags to keep track of what the hh is doing/has done
        self.resp_planned = False
        self.resp_sent = False
        self.resp_rec = False
        self.resp_time = 0
        self.status = ''
        self.visits = 0

        self.rep.env.process(self.action())

    def action(self):

        action_test = self.rnd.uniform(0, 100)  # represents the COA to be taken.

        if action_test <= self.resp_level:

            self.resp_planned = True

            # determine date and time of response
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

            response_date_time_hours = (response_date_time - current_date_time).total_seconds()/3600

            # wait until that time
            self.status = "Responding"
            yield self.env.timeout(response_date_time_hours)
            # then respond
            yield self.env.process(self.respond(self.delay))

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

        current_ad = yield self.rep.adviser_store.get()
        yield self.env.timeout(0.5)
        self.rep.adviser_store.put(current_ad)

    def respond(self, delay=0):
        """represents the hh responding - not the return being received by census"""

        if self.resp_sent is False:

            self.resp_sent = True
            self.resp_time = self.env.now
            # add to hh response event log

            self.output_data['Respond'].append(response(self.rep.reps,
                                                        self.district.name,
                                                        self.hh_id,
                                                        self.hh_type,
                                                        self.resp_time))

            if self.delay == 0:  # digital
                self.env.process(censusv2.ret_rec(self, self.rep))
            else:  # paper
                start_delayed(self.env, censusv2.ret_rec(self, self.rep), delay)

            yield self.env.timeout((self.rep.sim_hours) - self.env.now)  # hh does no more (without intervention)


# beta dist used to generate some numbers for responses over time
def beta_dist(rep, alpha, beta, sim_days_left):
    # return (rep.rnd.betavariate(alpha, beta))*(rep.sim_hours - rep.env.now)
    return int((rep.rnd.betavariate(alpha, beta))*sim_days_left)


def gauss_dist(rnd, alpha, beta):

    output = rnd.gauss(alpha, beta)

    if output < 0:
        output = 0

    return output


def set_preference(hh):
    """sets whether the hh prefers paper or digital and the associated time to receive responses from both"""
    paper_test = hh.rep.rnd.uniform(0, 100)

    if paper_test <= int(hh.input_data['paper_prop']):

        return "paper"

    return "digital"







