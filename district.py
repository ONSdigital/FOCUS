"""A district represents any user defined region (such as an LSOA/LA) that contains a set number of HH
and shares a number of census officers"""
import household
import sys
import hq
import math
from simpy.util import start_delayed
import output_options as oo
import helper as h
import FFU


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
        self.letters = []  # list of letters to be sent to hh in the district
        self.total_households = 0  # count of total including those not represented by objects
        # self.return_rate = 0  #
        self.travel_dist = 0  # average travel distance between hh for district
        self.early_responders = 0  # records number of hh who return prior to first interaction
        self.postal_delay = self.input_data['postal_delay']

        if self.input_data["census officer"]:
            self.create_co()
        if self.input_data["letter_phases"]:
            self.create_letterphases()  # set processes to start the sending of letters
        try:
            self.first_visit = min([co.start_sim_time for co in self.district_co])
        except ValueError as e:
            # if no visits then set to end of sim
            self.first_visit = self.rep.sim_hours

        try:
            self.first_letter = min([letter.start_sim_time for letter in self.letters])
        except ValueError as e:
            # if no letters then set to end of sim
            self.first_letter = self.rep.sim_hours

        if self.district_co:
            self.first_interaction = min(self.first_visit, self.first_letter)  # time of first interaction
            start_delayed(self.env, FFU.start_fu(self.env, self), math.floor(self.first_visit/24)*24)
        else:
            self.first_interaction = 0

        # create households that exist in the district
        self.create_households()
        # randomise list -  so ignore priority
        self.rnd.shuffle(self.households)
        try:
            self.hh_area = self.input_data['district_area'] / len(self.households)
            self.initial_hh_sep = 2 * (math.sqrt(self.hh_area / math.pi))
        except ZeroDivisionError as e:
            warning_detail = ("Zero division error in run: ", self.rep.run, ", rep: ", self.rep.reps,
                              " for district: ", self.name, ". HH separation set to zero")
            # write out a warning here but don't stop the sim just set the dist to zero
            if oo.record_warnings:
                self.rep.output_data['Warnings'].append(oo.warnings(self.rep.reps,
                                                                    e,
                                                                    warning_detail))

            self.initial_hh_sep = 0

        self.env.process(self.av_travel_dist())
        self.env.process(self.start_hh())

        start_delayed(self.env, self.non_response(), self.rep.sim_hours-0.0000001)

    # add process to collate info about non responding hh??

    def non_response(self):

        for household in self.households:
            if not household.return_sent:
                if oo.record_non_response:
                    self.rep.output_data['Non_response'].append(oo.generic_output(self.rep.reps,
                                                                                  self.name,
                                                                                  household.la,
                                                                                  household.lsoa,
                                                                                  household.digital,
                                                                                  household.hh_type,
                                                                                  household.hh_id,
                                                                                  self.env.now))
        yield self.env.timeout(0)

    def start_hh(self):
        # all start at the same time at present - in reality not all will receive IAC at same time
        for household in self.households:
            self.env.process(household.action())

        yield self.env.timeout((self.rep.sim_hours) - self.env.now)

    # takes current response rate and calculates average hh separation based on current response rate.
    def av_travel_dist(self):

        try:
            self.travel_dist = self.initial_hh_sep / (math.sqrt(1 - (h.responses_to_date(self, "%"))))
        except ZeroDivisionError as e:

            warning_detail = ("Zero division error in run: ", self.rep.run, ", rep: ", self.rep.reps,
                              " for district: ", self.name)
            # write out a warning here but don't stop the sim just set the dist to zero
            if oo.record_warnings:
                self.rep.output_data['Warnings'].append(oo.warnings(self.rep.reps,
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
                    self.district_co.append(FFU.CensusOfficer(self.rep,
                                                              self.env,
                                                              self,
                                                              co_input_data,
                                                              self.rep.total_co))

                    self.rep.total_co += 1

            except KeyError as e:
                print("Error when creating CO type", co_type, " in run: ", self.rep.run)
                sys.exit()

    def return_household_geog(self, input_dict):
        # returns LA and LSOA codes for hh

        for la in input_dict:
            for lsoa in input_dict[la]:
                if int(input_dict[la][lsoa]) > 0:
                    input_dict[la][lsoa] = int(input_dict[la][lsoa]) - 1
                    return oo.hh_geography(la, lsoa)

    def create_households(self):

        list_of_hh_types = sorted(list(self.input_data['households'].keys()))
        for hh_type in list_of_hh_types:

            # get hh data for current type
            hh_input_data = self.input_data['households'][hh_type]

            try:

                for i in range(hh_input_data['number']):

                    hh_geog = self.return_household_geog(hh_input_data['cca_makeup'])

                    self.total_households += 1

                    # determine initial HH action
                    hh_action = self.initial_action(hh_input_data, self.first_interaction, hh_type, hh_geog)

                    if hh_action.type == 'early':
                        # don't need an instance of a household just directly record response/return at correct time

                        self.rep.total_responses += 1
                        if oo.record_responded:
                            self.rep.output_data['Return_sent'].append(oo.generic_output(self.rep.reps,
                                                                                         self.name,
                                                                                         hh_geog.la,
                                                                                         hh_geog.lsoa,
                                                                                         hh_action.digital,
                                                                                         hh_type,
                                                                                         self.rep.total_hh,
                                                                                         hh_action.time))

                            if hh_action.digital:
                                time_to_use = hh_action.time + hh_input_data['delay']['digital']
                            else:
                                time_to_use = hh_action.time + hh_input_data['delay']['paper']

                            self.rep.output_data['Return_received'].append(oo.generic_output(self.rep.reps,
                                                                                             self.name,
                                                                                             hh_geog.la,
                                                                                             hh_geog.lsoa,
                                                                                             hh_action.digital,
                                                                                             hh_type,
                                                                                             self.rep.total_hh,
                                                                                             time_to_use))

                            self.rep.output_data['Responded'].append(oo.generic_output(self.rep.reps,
                                                                                       self.name,
                                                                                       hh_geog.la,
                                                                                       hh_geog.lsoa,
                                                                                       hh_action.digital,
                                                                                       hh_type,
                                                                                       self.rep.total_hh,
                                                                                       time_to_use))
                    else:
                        # create a household instance passing initial state
                        self.households.append(household.Household(self.rep,
                                                                   self.env,
                                                                   self,
                                                                   self.rep.total_hh,
                                                                   hh_type,
                                                                   hh_input_data,
                                                                   hh_action,
                                                                   hh_geog.la,
                                                                   hh_geog.lsoa))

                    if self.rep.reps == 1:
                        self.rep.output_data['hh_record'].append(oo.hh_record(self.name,
                                                                              hh_geog.la,
                                                                              hh_geog.lsoa,
                                                                              hh_type,
                                                                              hh_action.type,
                                                                              hh_action.digital,
                                                                              hh_action.time))
                    self.rep.total_hh += 1

            except KeyError as e:
                print("No key ", e, " for ", hh_type, " in district: ", self.name, " when creating hh")
                sys.exit()

    def create_letterphases(self):

        letter_list = sorted(list(self.input_data['letter_phases']))

        for letter in letter_list:
            letter_data = self.input_data['letter_phases'][letter]
            self.letters.append(hq.LetterPhase(self.env,
                                               self.rep,
                                               self,
                                               letter_data,
                                               letter))

    def initial_action(self, input_data, first_interaction, hh_type, hh_geog):

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
        # if call centre not in sim set call probability to zero even if input says otherwise
        if self.rep.total_ad_instances > 0:
            hh_help = input_data['behaviours'][behaviour]['help']
        else:
            hh_help = 0

        response_test = self.rnd.uniform(0, 100)  # represents the COA to be taken.

        if response_test <= hh_resp:
            # respond but test when
            return self.early_responder(input_data, digital, first_interaction, hh_type, hh_geog)

        elif hh_resp < response_test <= hh_resp + hh_help:
            # call for help return when
            return self.help(input_data, digital, first_interaction, hh_type, hh_geog)
        else:
            # do nothing return 0 time
            return oo.initial_action('do_nothing', digital, 0)
            # return self.do_nothing(input_data, digital, first_interaction, hh_type, hh_geog)

    def early_responder(self, input_data, digital, first_interaction, hh_type, hh_geog):
        # returns if the household will respond before any other interactions. "early" if yes "late" otherwise.

        response_time = h.set_household_response_time(self.rep,
                                                      input_data,
                                                      hh_type,
                                                      digital)

        if digital and response_time + input_data['delay']['digital'] <= first_interaction:

            # add a counter to the district so we know how many hh have responded early
            self.early_responders += 1

            return oo.initial_action('early', digital, response_time)

        elif not digital and h.str2bool(input_data['paper_allowed']) \
                and response_time + input_data['delay']['paper'] <= first_interaction:

            self.early_responders += 1
            return oo.initial_action('early', digital, response_time)

        else:

            return oo.initial_action('late', digital, response_time)

    def help(self, input_data, digital, first_interaction, hh, hh_geog):

        # below uses response time profile - will need to update this to a "call" profile?
        response_time = h.set_household_call_time(self.rep)

        return oo.initial_action('help', digital, response_time)

    #def do_nothing(self, input_data, digital, first_interaction, hh, hh_geog):

        #return oo.initial_action('do_nothing', digital, 0)


def least_busy_CO(district):

    min_length = min([len(co.action_plan) for co in district.district_co])

    coord = [co for co in district.district_co if len(co.action_plan) == min_length]

    return coord[0]






