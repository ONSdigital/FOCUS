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
warnings = namedtuple('Warnings', ['rep', 'warning', 'detail'])

# name tuples used for purposes other than output
initial_action = namedtuple('Initial_action', ['type', 'digital', 'time'])


class District(object):

    def __init__(self, rep, name):
        # values fed into class
        self.rep = rep
        self.name = name

        # created by and belong too the class
        self.rnd = self.rep.rnd
        self.env = self.rep.env
        self.input_data = self.rep.input_data['districts'][name]
        self.households = []  # list of household objects in the district
        self.district_co = []  # list of CO assigned to the district
        self.reminders = []  # list of reminders to be sent
        self.total_households = 0  # count of total including those not represented by objects
        # self.return_rate = 0  #
        self.travel_dist = 0  # average travel distance between hh for district
        self.early_responders = 0  # records number of hh who return prior to first interaction

        #self.create_co2(self.input_data["census officer"])  # process which creates co for district
        self.create_co()
        self.first_interaction = min([co.start_sim_time for co in self.district_co])  # time of first interaction
        start_delayed(self.env, censusv2.start_fu(self.env, self), math.floor(self.first_interaction/24)*24)

        # create households that exist in the district
        self.create_households()
        if self.rep.reps == 1:
            # record numbers for first replication
            self.rep.output_data['hh_count'].append(hh_count(self.name, self.total_households))

        # self.create_letterphases()

        try:
            self.hh_area = self.input_data['district_area'] / len(self.households)
            self.initial_hh_sep = 2 * (math.sqrt(self.hh_area / math.pi))
        except ZeroDivisionError as e:
            warning_detail = ("Zero division error in run: ", self.rep.run, ", rep: ", self.rep.reps,
                              " for district: ", self.name, ". HH separattion set to zero")
            # write out a warning here but don't stop the sim just set the dist to zero
            self.rep.output_data['Warnings'].append(warnings(self.rep.reps,
                                                             e,
                                                             warning_detail))

            self.initial_hh_sep = 0

        self.env.process(self.av_travel_dist())
        self.env.process(self.start_hh())

    def start_hh(self):
        # all start at the same time at present - in reality not all will receive IAC at same time
        for household in self.households:
            self.env.process(household.action())

        yield self.env.timeout((self.rep.sim_hours) - self.env.now)

    # takes current response rate and calculates average hh separation based on current response rate.
    def av_travel_dist(self):

        try:
            self.travel_dist = self.initial_hh_sep / (math.sqrt(1 - (returns_to_date(self, "%"))))
        except ZeroDivisionError as e:

            warning_detail = ("Zero division error in run: ", self.rep.run, ", rep: ", self.rep.reps,
                              " for district: ", self.name)
            # write out a warning here but don't stop the sim just set the dist to zero
            self.rep.output_data['Warnings'].append(warnings(self.rep.reps,
                                                             e,
                                                             warning_detail))

            self.travel_dist = 0

        yield self.env.timeout(24)
        self.env.process(self.av_travel_dist())

    def create_co(self):

        id_num = 0
        list_of_co_types = sorted(list(self.input_data['census officer'].keys()))
        for co_type in list_of_co_types:

            # get hh data for current type
            co_input_data = self.input_data['census officer'][co_type]

            try:

                for i in range(int(co_input_data['number'])):

                    id_num += 1
                    self.district_co.append(censusv2.CensusOfficer(self.rep,
                                                                   self.env,
                                                                   self,
                                                                   co_input_data,
                                                                   self.rep.total_co))

                    self.rep.total_co += 1

            except KeyError as e:
                print("No key called number for CO in district: ", self.name)
                sys.exit()

    def create_co2(self, input_data, input_key=""):

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

                except KeyError as e:
                    print(e, "No number key for co in district: ", self.name)
                    sys.exit()
                break

        return input_key

    def create_households(self):

        list_of_hh_types = sorted(list(self.input_data['households'].keys()))
        for hh in list_of_hh_types:

            # get hh data for current type
            hh_input_data = self.input_data['households'][hh]

            try:

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

            except KeyError as e:
                print(e, "No key called number for HH in district: ", self.name)
                sys.exit()

    #def start_fu(self):

        # determines when FU starts for the district - ie, when does the first CO become available
     #   start_delayed(self.env, censusv2.start_fu(self.env, self), self.first_interaction)

    def create_letterphases(self):

        letter_list = sorted(list(self.input_data['letter_phases']))

        for letter in letter_list:
            letter_data = self.input_data['letter_phases'][letter]
            self.reminders.append(censusv2.LetterPhase(self.env,
                                                       self.rep,
                                                       self,
                                                       letter_data))

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
            # NEED TO SET OR PASS THAT THE HH PLANS TO RESPOND if not an early responder


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

            # add a counter to the district so we know how many hh have responded early
            self.early_responders += 1

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

            self.early_responders += 1
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






