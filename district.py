"""A district represents any user defined region (such as an LSOA/LA) that contains a set number of HH
and shares a number of census officers"""
import householdv2
import sys
import censusv2
import math
from helper import returns_to_date
from simpy.util import start_delayed
from collections import namedtuple
import helper as h

hh_count = namedtuple('hh_count', ['district', 'hh_count'])
hh_record = namedtuple('hh_record', ['district', 'hh_type'])
return_times = namedtuple('Returned', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])
response = namedtuple('Response', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])

# name tuples used for purposes other than output
initial_action = namedtuple('Initial_action', ['type', 'digital', 'time'])


class District(object):

    def __init__(self, rep, name):
        # values fed into class
        self.rep = rep
        self.rnd = self.rep.rnd
        self.env = self.rep.env
        self.name = name
        self.input_data = self.rep.input_data['districts'][name]

        # belong to the class
        self.households = []  # list of household objects in the district
        self.total_households = 0  # count of total including those not represented by objects
        self.district_co = []  # list of CO assigned to the district
        self.reminders = []  # list of reminders to be sent
        self.return_rate = 0
        self.travel_dist = 0

        self.create_co(self.input_data["census officer"])
        self.first_interaction = min([co.start_simpy_time for co in self.district_co])
        start_delayed(self.env, censusv2.start_fu(self.env, self), math.floor(self.first_interaction/24)*24)

        # create households that exist in the district
        self.create_households()
        if self.rep.reps == 1:
            self.rep.output_data['hh_count'].append(hh_count(self.name, self.total_households))

        # self.create_letterphases()

        try:
            self.hh_area = self.input_data['district_area'] / len(self.households)
            self.initial_hh_sep = 2 * (math.sqrt(self.hh_area / math.pi))
        except ZeroDivisionError as e:
            print(e, " in district ", self.name)
            sys.exit()

        self.env.process(self.hh_separation())
        self.env.process(self.start_hh())

    def start_hh(self):
        # all start at the same time at present - in reality not all will receive IAC at same time
        for household in self.households:
            self.env.process(household.action())

        yield self.env.timeout((self.rep.sim_hours) - self.env.now)

    # takes current response rate and calculates average hh separation based on current response rate.
    def hh_separation(self):

        while True:

            try:
                self.travel_dist = self.initial_hh_sep / (math.sqrt(1 - (returns_to_date(self, "%"))))
            except ZeroDivisionError:
                self.travel_dist = 0

            yield self.env.timeout(24)

    def create_households(self):

        list_of_hh = sorted(list(self.input_data['households'].keys()))
        for hh in list_of_hh:

            # get hh data for current type
            hh_input_data = self.input_data['households'][hh]

            for i in range(hh_input_data['number']):

                self.total_households += 1

                # determine initial HH action
                hh_action = self.initial_action(hh_input_data, self.first_interaction, hh)

                if hh_action.type == 'early':
                    # don't need an instance of a household just directly record a response/return at correct time

                    self.rep.total_returns += 1

                    self.rep.output_data['Returned'].append(return_times(self.rep.reps,
                                                                         self.name,
                                                                         hh_input_data["LA"],
                                                                         hh_input_data["LSOA"],
                                                                         hh_action.digital,
                                                                         hh,
                                                                         None,
                                                                         hh_action.time))
                else:
                    # create a household instance passing initial state
                    self.households.append(householdv2.Household(self.rep,
                                                                 self.rnd,
                                                                 self.env,
                                                                 self,
                                                                 self.rep.total_hh,
                                                                 hh,
                                                                 hh_input_data,
                                                                 hh_action))

                if self.rep.reps == 1:
                    self.rep.output_data['hh_record'].append(hh_record(self.name,
                                                                       hh))
                self.rep.total_hh += 1

    def start_fu(self):

        # determines when FU starts for the district - ie, when does the first CO become available
        start_delayed(self.env, censusv2.start_fu(self.env, self), self.first_interaction)

    def create_letterphases(self):

        letter_list = sorted(list(self.input_data['letter_phases']))

        for letter in letter_list:
            letter_data = self.input_data['letter_phases'][letter]
            self.reminders.append(censusv2.LetterPhase(self.env,
                                                       self.rep,
                                                       self,
                                                       letter_data))

    def create_co(self, input_data, input_key=""):

        id_num = 0
        for key, value in input_data.items():
            if isinstance(value, dict):

                self.create_co(value, key)

            else:
                try:
                    if 'number' in input_data:

                        for i in range(int(input_data["number"])):

                            id_num += 1
                            self.district_co.append(censusv2.CensusOfficer(self.rep,
                                                                           self.env,
                                                                           self,
                                                                           input_data,
                                                                           self.rep.total_co))

                            self.rep.total_co += 1

                except IOError as e:
                    print(e)
                    sys.exit()
                break

        return input_key

    def initial_action(self, input_data, first_interaction, hh):

        digital = h.set_preference(input_data['paper_prop'],
                                   self.rnd)

        if digital or h.str2bool(input_data['paper_allowed']):
            # use default
            behaviour = 'default'
        else:
            # use alt
            behaviour = 'alt'

        # set values to use
        hh_resp = input_data['behaviours'][behaviour]['response']
        hh_help = input_data['behaviours'][behaviour]['help']

        response_test = self.rnd.uniform(0, 100)  # represents the COA to be taken.
        if response_test <= hh_resp:
            # respond but test when
            return self.early_responder(input_data, digital, first_interaction, hh)
            # NEED TO SET OR PASS THAT THE HH PLANS TO RESPOND


        elif hh_resp < response_test <= hh_help:
            # call for help return when
            return self.help(input_data, digital, first_interaction, hh)
        else:
            # do nothing return 0 time
            return self.do_nothing(input_data, digital, first_interaction, hh)

    def early_responder(self, input_data, digital, first_interaction, hh):

        response_time = h.set_household_response_time(self.rep,
                                                      input_data,
                                                      self.rep.sim_hours)

        if digital and response_time + input_data['delay']['digital'] <= first_interaction:

            self.rep.output_data['Response'].append(response(self.rep.reps,
                                                             self.name,
                                                             input_data["LA"],
                                                             input_data["LSOA"],
                                                             digital,
                                                             hh,
                                                             None,
                                                             response_time))

            return initial_action('early', digital, response_time + input_data['delay']['digital'])

        elif not digital and h.str2bool(input_data['paper_allowed']) \
                and response_time + input_data['delay']['paper'] <= first_interaction:

            self.rep.output_data['Response'].append(response(self.rep.reps,
                                                             self.name,
                                                             input_data["LA"],
                                                             input_data["LSOA"],
                                                             digital,
                                                             hh,
                                                             None,
                                                             response_time))

            return initial_action('early', digital, response_time + input_data['delay']['paper'])

        else:

            return initial_action('late', digital, response_time)

    def help(self, input_data, digital, first_interaction, hh):

        return initial_action('help', digital, 0)

    def do_nothing(self, input_data, digital, first_interaction, hh):

        return initial_action('do_nothing', digital, 0)


def least_busy_CO(district):

    min_length = min([len(co.action_plan) for co in district.district_co])

    coord = [co for co in district.district_co if len(co.action_plan) == min_length]

    return coord[0]






