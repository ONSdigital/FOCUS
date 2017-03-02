"""file used to store the classes and definitions related to the households represented in the simulation """

import hq
from simpy.util import start_delayed
from collections import namedtuple
import helper as h
import math
import datetime as dt

generic_output = namedtuple('Generic_output', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])
reminder_wasted = namedtuple('Reminder_wasted', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time','type'])
reminder_unnecessary = namedtuple('Reminder_unnecessary', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time', 'type'])
reminder_success = namedtuple('Reminder_success', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time', 'type'])
reminder_received = namedtuple('Reminder_received', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time', 'type'])
call_wait_times = namedtuple('Call_wait_times', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time', 'wait_time'])



class Household(object):

    # Create an instance of the class
    def __init__(self, rep, env, district, hh_id, hh_type, input_data, initial_action, la, lsoa):

        self.rep = rep
        self.rnd = self.rep.rnd
        self.env = env
        self.district = district
        self.hh_id = hh_id
        self.hh_type = hh_type
        self.input_data = input_data
        self.la = la
        self.lsoa = lsoa

        self.output_data = self.rep.output_data
        self.initial_status = initial_action.type
        self.digital = initial_action.digital
        self.initial_time = initial_action.time

        self.priority = self.input_data['priority']
        self.paper_allowed = h.str2bool(self.input_data['paper_allowed'])
        self.paper_on_request = h.str2bool(self.input_data['paper_on_request'])

        # flags to keep track of what the hh is doing/has done

        if self.initial_time > 0 and (self.initial_status == 'early' or self.initial_status == 'late'):
            self.resp_planned = True
        else:
            self.resp_planned = False

        self.resp_time = 0
        self.resp_level = 0
        self.help_level = 0
        self.responded = False
        self.return_sent = False
        self.return_received = False
        self.status = ''
        self.visits = 0
        self.visits_contacted = 0
        self.calls = 0
        self.arranged_visit = False
        self.letter_count = 0

    def action(self):
        # directs the household to follow the COA set on initialisation at the relevant time

        yield self.env.timeout(self.initial_time)

        if self.initial_status == 'late' and not self.return_sent:
            # normal response
            yield self.env.process(self.household_returns(self.calc_delay()))

        elif self.initial_status == 'help' and not self.return_sent:
            # help
            yield self.env.process(self.contact())

        else:
            # nowt
            self.output_data['Do_nothing'].append(generic_output(self.rep.reps,
                                                                 self.district.name,
                                                                 self.la,
                                                                 self.lsoa,
                                                                 self.digital,
                                                                 self.hh_type,
                                                                 self.hh_id,
                                                                 self.env.now))
        yield self.env.timeout(0)  # do nothing more

    def contact(self):
        # routing for the type of contact

        if not self.responded:

            yield self.env.process(self.phone_call())

    def phone_call(self):

        self.output_data['Call'].append(generic_output(self.rep.reps,
                                                       self.district.name,
                                                       self.la,
                                                       self.lsoa,
                                                       self.digital,
                                                       self.hh_type,
                                                       self.hh_id,
                                                       self.env.now))
        self.calls += 1

        if (not self.digital and not self.paper_allowed and
            h.responses_to_date(self.district) < self.district.input_data['paper_trigger'] and self.paper_on_request):
            # so provide paper if the conditions are met...

            self.paper_allowed = True
            self.priority += 5  # lower the priority as more likely to reply
            hq.schedule_paper_drop(self, 'Call', 'pq', False)

            yield self.env.timeout(0)

        elif isinstance(self.adviser_check(self.env.now), str):
            # otherwise carry on to the call centre to speak to an adviser
            # unless outside times advisers are available - use check times and if none returned gracefully defer
            yield self.env.process(self.phone_call_connect())

        else:
            # no one available - gracefully defer - some will call back again
            self.output_data['Call_defer'].append(generic_output(self.rep.reps,
                                                              self.district.name,
                                                              self.la,
                                                              self.lsoa,
                                                              self.digital,
                                                              self.hh_type,
                                                              self.hh_id,
                                                              self.env.now))

    def default_behaviour(self):
        # depending on circumstances can follow one of two paths
        if self.digital or self.paper_allowed:
            return 'default'
        else:
            return 'alt'

    def action_test(self, type):
        # tests what a household will do next after an interaction
        # returns action and time of action.

        test_value = self.rnd.uniform(0, 100)
        # test if default or alt
        behaviour = self.default_behaviour()

        dict_value = self.input_data["behaviours"][type]

        if test_value <= dict_value["response"]:
            # respond straight away
            return ["respond", self.env.now]
        elif dict_value["response"] < test_value <= dict_value["response"] + dict_value["help"]:
            # or call again sometime later, set at 1 hour later for now...
            return ["contact", 1]
        else:
            return ["Do nothing", 0]

    def adviser_check(self, time):

        for k, v in self.rep.adviser_types.items():
            # between the dates
            if (v['start_time'] < time <= v['end_time']):

                # then between each set of times
                current_dow = str((self.rep.start_date + dt.timedelta(days=math.floor(time / 24))).weekday())
                for i in range(0, len(v['availability'][current_dow]), 2):
                    in_time = h.make_time_decimal(dt.time(*map(int, v['availability'][current_dow][i].split(':'))))
                    out_time = h.make_time_decimal(dt.time(*map(int, v['availability'][current_dow][i+1].split(':'))))

                    if in_time <= self.env.now % 24 < out_time:
                        return k

        return None

        # cycle through adviser types
        # get adviser type that is between dates

        # for that type is it available

    def phone_call_connect(self):

        # speak to someone - go from here to a more complex assist/outcome section
        called_at = self.env.now

        # at this time/date what type of adviser is available?
        # is one of these types available?
        adviser = self.adviser_check(called_at)

        current_ad = yield self.rep.adviser_store.get(lambda item: item.type == adviser)
        wait_time = self.env.now - called_at

        # renege time a fixed value for now. Determine suitable distribution from paradata or call centre MI.
        if wait_time >= self.input_data["renege"]:
            # hang up

            self.output_data['Call_renege'].append(generic_output(self.rep.reps,
                                                               self.district.name,
                                                               self.la,
                                                               self.lsoa,
                                                               self.digital,
                                                               self.hh_type,
                                                               self.hh_id,
                                                               self.env.now))

            self.rep.adviser_store.put(current_ad)

            # record wait times for stats generation - wait time her eis how long they would of had to wait
            # not how long they did wait - this is given by the renege time
            if wait_time > 0:
                self.record_wait_time(wait_time, self.input_data["renege"])

            # so hh didn't get through - some may now respond with no extra tries but more will call again or give up.
            action = self.action_test("renege")
            # but when?
            if not action[0] == "Do nothing":
                function_to_call = getattr(self, action[0])
                yield self.env.timeout(action[1])
                yield self.env.process(function_to_call())

        else:
            # got through

            self.output_data['Call_contact'].append(generic_output(self.rep.reps,
                                                                 self.district.name,
                                                                 self.la,
                                                                 self.lsoa,
                                                                 self.digital,
                                                                 self.hh_type,
                                                                 self.hh_id,
                                                                 self.env.now))

            if wait_time > 0:
                self.record_wait_time(wait_time, self.input_data["renege"])

            yield self.env.process(self.phone_call_assist(current_ad))

    def phone_call_assist(self, current_ad):

        da_test = self.rnd.uniform(0, 100)
        # how effective current adviser type at conversion to digital
        da_effectiveness = current_ad.input_data['da_effectiveness'][self.hh_type]

        # wait to determine if digital or paper preference
        yield self.env.timeout(current_ad.input_data['call_times']['query'] / 60)

        if self.digital:
            # no need to persuade go straight to the outcome
            yield self.rep.env.process(self.phone_call_outcome(current_ad))

        elif not self.digital and da_test <= da_effectiveness:
            # not digital and adviser converts household to digital but takes some time
            yield self.env.timeout(current_ad.input_data['call_times']['convert'] / 60)

            # record event
            self.rep.output_data['Call_convert'].append(generic_output(self.rep.reps,
                                                                     self.district.name,
                                                                     self.la,
                                                                     self.lsoa,
                                                                     self.digital,
                                                                     self.hh_type,
                                                                     self.hh_id,
                                                                     self.rep.env.now))

            self.digital = True
            yield self.env.process(self.phone_call_outcome(current_ad))

        elif not self.digital and da_test > da_effectiveness:
            # not digital and not converted so arrange a visit or something else when we know what!

            yield self.env.timeout(current_ad.input_data['call_times']['failed'] / 60)

            self.rep.output_data['call_request'].append(generic_output(self.rep.reps,
                                                                     self.district.name,
                                                                     self.la,
                                                                     self.lsoa,
                                                                     self.digital,
                                                                     self.hh_type,
                                                                     self.hh_id,
                                                                     self.rep.env.now))

            # up priority and tag so a visit happens at time likely time to be in - or just set it to succeed - or both?
            self.priority -= 10

            self.rep.adviser_store.put(current_ad)
            #self.arranged_visit = True  # basically has requested a visit so go at optimal time and move to front..
            self.arranged_visit = False

    def phone_call_outcome(self, current_ad):

        # digital by this point so just can you convince them to reply?
        outcome_test = self.rnd.uniform(0, 100)
        conversion_dict = self.input_data['conversion_rate'][str(h.current_day(self))]

        if outcome_test <= conversion_dict[h.return_time_key(conversion_dict, self.env.now)]:

            yield self.env.timeout(current_ad.input_data['call_times']['success'] / 60)

            self.rep.output_data['Call_success'].append(generic_output(self.rep.reps,
                                                                     self.district.name,
                                                                     self.la,
                                                                     self.lsoa,
                                                                     self.digital,
                                                                     self.hh_type,
                                                                     self.hh_id,
                                                                     self.env.now))
            self.resp_planned = True
            self.rep.adviser_store.put(current_ad)
            self.rep.env.process(self.household_returns(self.calc_delay()))

        else:

            yield self.env.timeout(current_ad.input_data['call_times']['failed'] / 60)

            self.rep.output_data['Call_failed'].append(generic_output(self.rep.reps,
                                                                   self.district.name,
                                                                   self.la,
                                                                   self.lsoa,
                                                                   self.digital,
                                                                   self.hh_type,
                                                                   self.hh_id,
                                                                   self.env.now))

            self.rep.adviser_store.put(current_ad)

    def household_returns(self, delay=0):
        """represents the hh returning their form - not the return being counted as a response by census"""

        if not self.return_sent:

            self.return_sent = True
            self.resp_time = self.env.now
            # add to hh response event log

            self.output_data['Return_sent'].append(generic_output(self.rep.reps,
                                                               self.district.name,
                                                               self.la,
                                                               self.lsoa,
                                                               self.digital,
                                                               self.hh_type,
                                                               self.hh_id,
                                                               self.resp_time))

            if self.calc_delay() == 0:  # digital
                self.env.process(hq.ret_rec(self, self.rep))
            else:  # paper
                start_delayed(self.env, hq.ret_rec(self, self.rep), delay)

            yield self.env.timeout(0)  # hh does no more (without intervention)

    def receive_reminder(self, reminder_type):
        # a reminder has been received. This determines the outcome fo that reminder and if it was worthwhile.
        # if a pq set paper allowed to true!
        if reminder_type == 'pq':
            self.paper_allowed = True

        behaviour = self.default_behaviour()
        # and get relevant figures
        response_data = self.input_data["behaviours"][reminder_type][behaviour]
        self.resp_level = response_data["response"]
        if self.rep.total_ad_instances > 0:
            self.help_level = response_data["help"]
        else:
            self.help_level = 0

        # recorded if wasted, unnecessary or successful
        if self.responded:

            self.rep.output_data['Reminder_wasted'].append(reminder_wasted(self.rep.reps,
                                                                           self.district.name,
                                                                           self.la,
                                                                           self.lsoa,
                                                                           self.digital,
                                                                           self.hh_type,
                                                                           self.hh_id,
                                                                           self.env.now,
                                                                           reminder_type))
        elif self.resp_planned:

            self.rep.output_data['Reminder_unnecessary'].append(reminder_unnecessary(self.rep.reps,
                                                                                     self.district.name,
                                                                                     self.la,
                                                                                     self.lsoa,
                                                                                     self.digital,
                                                                                     self.hh_type,
                                                                                     self.hh_id,
                                                                                     self.env.now,
                                                                                     reminder_type))
        else:
            # if get here they have not responded or planned to do so, so a worthwhile reminder.
            self.rep.output_data['Reminder_received'].append(reminder_success(self.rep.reps,
                                                                              self.district.name,
                                                                              self.la,
                                                                              self.lsoa,
                                                                              self.digital,
                                                                              self.hh_type,
                                                                              self.hh_id,
                                                                              self.env.now,
                                                                              reminder_type))

        # now move on to the relevant action based on extracted values
        # response test
        reminder_test = self.rnd.uniform(0, 100)

        if not self.responded and reminder_test <= self.resp_level:

            self.rep.output_data['Reminder_success'].append(reminder_success(self.rep.reps,
                                                                             self.district.name,
                                                                             self.la,
                                                                             self.lsoa,
                                                                             self.digital,
                                                                             self.hh_type,
                                                                             self.hh_id,
                                                                             self.env.now,
                                                                             reminder_type))

            yield self.env.process(self.household_returns(self.calc_delay()))
        elif not self.responded and (self.resp_level < reminder_test <= self.resp_level + self.help_level):
            # call for help...needs to be based on appropriate distribution...not a hardcoded uniform function!
            # also may not do this if intend to respond?
            yield self.env.timeout(self.rnd.uniform(0, 8))
            yield self.env.process(self.contact())
        elif not self.responded:
            # nowt
            self.output_data['Do_nothing'].append(generic_output(self.rep.reps,
                                                             self.district.name,
                                                             self.la,
                                                             self.lsoa,
                                                             self.digital,
                                                             self.hh_type,
                                                             self.hh_id,
                                                             self.env.now))

        yield self.env.timeout(0)

    def record_wait_time(self, wait_time, renege_time):

        self.output_data['Call_wait_times'].append(call_wait_times(self.rep.reps,
                                                                   self.district.name,
                                                                   self.la,
                                                                   self.lsoa,
                                                                   self.digital,
                                                                   self.hh_type,
                                                                   self.hh_id,
                                                                   self.env.now,
                                                                   min(wait_time, renege_time)))

    def calc_delay(self):
        if self.digital:
            return self.input_data["delay"]["digital"]
        else:
            return self.input_data["delay"]["paper"]

