"""file used to store the classes and definitions related to the households represented in the simulation """

import datetime
import censusv2
from simpy.util import start_delayed
from collections import namedtuple
import helper as h

response = namedtuple('Responded', ['reps', 'district', 'digital', 'hh_type', 'time'])
response_planned = namedtuple('Response_planned', ['reps', 'district', 'digital', 'hh_type', 'time'])
do_nothing = namedtuple('Do_nothing', ['reps', 'district', 'digital', 'hh_type', 'time'])
reminder_wasted = namedtuple('Reminder_wasted', ['rep', 'district', 'digital', 'hh_type', 'time', 'type'])
reminder_unnecessary = namedtuple('Reminder_unnecessary', ['rep', 'district', 'digital', 'hh_type', 'time', 'type'])


class Household(object):

    # Create an instance of the class
    def __init__(self, rep, rnd, env, district, id, hh_type, input_data, output_data):

        self.rep = rep
        self.rnd = rnd
        self.env = env
        self.district = district
        self.id = id
        self.hh_type = hh_type
        self.input_data = input_data
        self.output_data = output_data

        self.digital = set_preference(self)

        if self.digital:
            self.delay = self.input_data['delay']['digital']
        else:
            self.delay = self.input_data['delay']['paper']

        self.priority = self.input_data['priority']
        self.paper_allowed = h.str2bool(self.input_data['paper_allowed'])

        # flags to keep track of what the hh is doing/has done
        self.resp_planned = False
        self.responded = False
        self.returned = False
        self.resp_time = 0
        self.status = ''
        self.visits = 0

        self.resp_level = 0
        self.help_level = 0
        # below define how the hh behaves depending on preference

        self.resp_level = self.set_behaviour('response')
        self.help_level = self.set_behaviour('help')

        self.rep.env.process(self.action())

    def action(self):

        action_test = self.rnd.uniform(0, 100)  # represents the COA to be taken.

        if action_test <= self.resp_level and not self.responded:

            if not self.resp_planned and self.env.now == 0:

                self.output_data['Response_planned'].append(response_planned(self.rep.reps,
                                                                             self.district.name,
                                                                             self.digital,
                                                                             self.hh_type,
                                                                             self.env.now))
            self.resp_planned = True

            # determine date and time of response
            current_date_time = self.rep.start_date + datetime.timedelta(hours=self.rep.env.now)
            sim_days_left = (self.rep.end_date.date() - current_date_time.date()).days

            days_until_response = h.beta_dist(self.rep,
                                              self.input_data['beta_dist'][0],
                                              self.input_data['beta_dist'][1],
                                              sim_days_left)

            response_date = current_date_time.date() + datetime.timedelta(days=days_until_response)

            response_day = response_date.weekday()
            response_time = h.gauss_dist(self.rnd,
                                         self.input_data['response_time'][str(response_day)][0],
                                         self.input_data['response_time'][str(response_day)][1])

            response_date_time = datetime.datetime.combine(response_date, datetime.datetime.min.time())\
                                + datetime.timedelta(hours=response_time)
            # issue here with response time - gauss dist can create -ve time iof action is run during day
            response_date_time_hours = max((response_date_time - current_date_time).total_seconds()/3600, 0)

            self.status = "Responding"
            yield self.env.timeout(response_date_time_hours)
            # then respond
            yield self.env.process(self.respond(self.delay))

        elif self.resp_level < action_test <= self.help_level and not self.responded:

            contact_time = 10.5  # all call at same time for testing really some dist...from call centre patterns
            self.status = "making contact"
            yield self.env.timeout(contact_time)
            yield self.env.process(self.contact())

        elif not self.responded:
            self.output_data['Do_nothing'].append(do_nothing(self.rep.reps,
                                                             self.district.name,
                                                             self.digital,
                                                             self.hh_type,
                                                             self.env.now))

            self.status = "Do nothing"
            yield self.env.timeout((self.rep.sim_hours) - self.env.now)  # do nothing more

    def contact(self):

        current_ad = yield self.rep.adviser_store.get()
        print(self.id, ' got through at ', self.env.now)
        yield self.env.timeout(0.5)
        print(self.id, ' call ended at ', self.env.now)
        self.rep.adviser_store.put(current_ad)

    def respond(self, delay=0):
        """represents the hh responding - not the return being received by census"""

        if self.responded is False:

            self.responded = True
            self.resp_time = self.env.now
            # add to hh response event log

            self.output_data['Respond'].append(response(self.rep.reps,
                                                        self.district.name,
                                                        self.digital,
                                                        self.hh_type,
                                                        self.resp_time))

            if self.delay == 0:  # digital
                self.env.process(censusv2.ret_rec(self, self.rep))
            else:  # paper
                start_delayed(self.env, censusv2.ret_rec(self, self.rep), delay)

            yield self.env.timeout((self.rep.sim_hours) - self.env.now)  # hh does no more (without intervention)

    def receive_reminder(self, reminder_type):

        self.status = "received reminder"

        if self.resp_planned:

            if self.resp_planned and self.responded:
                self.rep.output_data['Reminder_wasted'].append(reminder_wasted(self.rep.reps,
                                                                               self.district.name,
                                                                               self.digital,
                                                                               self.hh_type,
                                                                               self.env.now,
                                                                               reminder_type))
            elif self.resp_planned and not self.responded:

                self.rep.output_data['Reminder_unnecessary'].append(reminder_unnecessary(self.rep.reps,
                                                                                         self.district.name,
                                                                                         self.digital,
                                                                                         self.hh_type,
                                                                                         self.env.now,
                                                                                         reminder_type))
            yield self.env.timeout(0)

        else:

            if not self.responded and reminder_type == 'pq' and not self.digital:
                # hh who have chosen not to respond due to not having paper are now given paper

                self.resp_level = self.set_behaviour('response')
                self.help_level = self.set_behaviour('help')

            elif not self.responded and reminder_type == 'pq' and self.digital:
                # hh who have chosen not to respond despite happy toi use digital - paper makes no/little difference?

                self.resp_level = 0
                self.help_level = 0

            else:
                # some other reminder received - will need to understand the effectiveness of these whatever they are
                self.resp_level = 0
                self.help_level = 0

            yield self.env.process(self.action())

    def set_behaviour(self, behaviour):

        if self.digital or self.paper_allowed is True:
            # use default
            return self.input_data['behaviours']['default'][behaviour]
        else:
            return self.input_data['behaviours']['alt'][behaviour]


def set_preference(household):
    """sets whether the hh prefers paper or digital and the associated time to receive responses from both"""
    paper_test = household.rep.rnd.uniform(0, 100)

    if paper_test <= int(household.input_data['paper_prop']):

        return False

    return True







