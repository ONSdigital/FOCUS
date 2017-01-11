"""file used to store the classes and definitions related to the households represented in the simulation """

import datetime
import censusv2
from simpy.util import start_delayed
from collections import namedtuple
import helper as h
import math
import district

response = namedtuple('Response', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])
response_planned = namedtuple('Response_planned', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])
do_nothing = namedtuple('Do_nothing', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])
reminder_wasted = namedtuple('Reminder_wasted', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time','type'])
reminder_unnecessary = namedtuple('Reminder_unnecessary', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time', 'type'])
reminder_success = namedtuple('Reminder_success', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time', 'type'])
call = namedtuple('Call', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])
call_renege = namedtuple('Call_renege', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])
call_contact = namedtuple('Call_contact', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])
call_wait_times = namedtuple('Call_wait_times', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time', 'wait_time'])
call_convert = namedtuple('Call_convert', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])
call_success = namedtuple('Call_success', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])
call_failed = namedtuple('Call_failed', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])
received_letter = namedtuple('Received_letter', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])
wasted_letter = namedtuple('Wasted_letter', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])
received_pq = namedtuple('Received_pq', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])
wasted_pq = namedtuple('Wasted_pq', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])
visit_request = namedtuple('Visit_request', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])


class Household(object):

    # Create an instance of the class
    def __init__(self, rep, env, district, hh_id, hh_type, input_data, initial_action):

        self.rep = rep
        self.rnd = self.rep.rnd
        self.env = env
        self.district = district
        self.hh_id = hh_id
        self.hh_type = hh_type
        self.input_data = input_data

        self.output_data = self.rep.output_data
        self.initial_status = initial_action.type
        self.digital = initial_action.digital
        self.initial_time = initial_action.time

        if self.digital:
            self.delay = self.input_data['delay']['digital']
        else:
            self.delay = self.input_data['delay']['paper']

        self.priority = self.input_data['priority']
        self.paper_allowed = h.str2bool(self.input_data['paper_allowed'])

        # flags to keep track of what the hh is doing/has done

        if self.initial_time > 0:
            self.resp_planned = True
        else:
            self.resp_planned = False

        self.resp_time = 0
        self.responded = False
        self.returned = False
        self.status = ''
        self.visits = 0
        self.visit_times = []
        self.calls = 0
        self.arranged_visit = False
        self.letter_count = 0

    def action(self):
        # directs the household to follow the COA set on initialisation at the relevant time

        yield self.env.timeout(self.initial_time)

        if self.initial_status == 'late' and not self.responded:
            # normal response
            yield self.env.process(self.respond(self.delay))

        elif self.initial_status == 'help' and not self.responded:
            # help
            yield self.env.process(self.contact())

        else:
            # nowt
            self.output_data['Do_nothing'].append(do_nothing(self.rep.reps,
                                                             self.district.name,
                                                             self.input_data["LA"],
                                                             self.input_data["LSOA"],
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

        self.output_data['Call'].append(call(self.rep.reps,
                                             self.district.name,
                                             self.input_data["LA"],
                                             self.input_data["LSOA"],
                                             self.digital,
                                             self.hh_type,
                                             self.hh_id,
                                             self.env.now))
        self.calls += 1

        if (not self.digital and not self.paper_allowed and
            h.returns_to_date(self.district) < self.district.input_data['paper_trigger'] and
            h.str2bool(self.input_data['paper_on_request'])):
            # so provide paper if the conditions are met...

            self.paper_allowed = True
            censusv2.schedule_paper_drop(self, self, False)

            yield self.env.timeout(0)

        else:
            # otherwise carry on to the call centre to speak to an adviser

            yield self.env.process(self.phone_call_connect())

    def phone_call_connect(self):

        # speak to someone - go from here to a more complex assist/outcome section
        called_at = self.env.now
        current_ad = yield self.rep.adviser_store.get()
        wait_time = self.env.now - called_at

        if wait_time >= h.renege_time(self):
            # hang up

            self.output_data['Call_renege'].append(call_renege(self.rep.reps,
                                                               self.district.name,
                                                               self.input_data["LA"],
                                                               self.input_data["LSOA"],
                                                               self.digital,
                                                               self.hh_type,
                                                               self.hh_id,
                                                               self.env.now))

            self.resp_level = 0
            self.help_level = self.resp_level + (self.set_behaviour('help', self.calls) * 0.5)
            self.rep.adviser_store.put(current_ad)
            if wait_time > 0:

                self.record_wait_time(wait_time, h.renege_time(self))

            yield self.env.process(self.action())

        else:
            # got through

            self.output_data['Call_contact'].append(call_contact(self.rep.reps,
                                                                 self.district.name,
                                                                 self.input_data["LA"],
                                                                 self.input_data["LSOA"],
                                                                 self.digital,
                                                                 self.hh_type,
                                                                 self.hh_id,
                                                                 self.env.now))

            if wait_time > 0:
                self.record_wait_time(wait_time, h.renege_time(self))

            yield self.env.process(self.phone_call_assist(current_ad))

    def phone_call_assist(self, current_ad):

        # similar to CO?
        # got to here which means either a problem or want paper but not allowed it...
        # if a problem go straight to outcome
        # else try to convert?

        da_test = self.rnd.uniform(0, 100)
        da_effectiveness = current_ad.input_data['da_effectiveness'][self.hh_type]

        # if digital or have already responded skip straight to the outcome of the visit
        if self.digital:

            yield self.env.timeout(current_ad.input_data['call_times']['query'] / 60)

            yield self.rep.env.process(self.phone_call_outcome(current_ad))

        elif not self.digital and da_test <= da_effectiveness:
            # want paper but not allowed it...but do convert to digital

            yield self.env.timeout(current_ad.input_data['call_times']['convert'] / 60)

            self.rep.output_data['Call_convert'].append(call_convert(self.rep.reps,
                                                                     self.district.name,
                                                                     self.input_data["LA"],
                                                                     self.input_data["LSOA"],
                                                                     self.digital,
                                                                     self.hh_type,
                                                                     self.hh_id,
                                                                     self.rep.env.now))

            self.digital = True
            yield self.env.process(self.phone_call_outcome(current_ad))

        elif not self.digital and da_test > da_effectiveness:
            # not digital and not converted so arrange a visit

            self.rep.output_data['Visit_request'].append(call_convert(self.rep.reps,
                                                                      self.district.name,
                                                                      self.input_data["LA"],
                                                                      self.input_data["LSOA"],
                                                                      self.digital,
                                                                      self.hh_type,
                                                                      self.hh_id,
                                                                      self.rep.env.now))

            yield self.env.timeout(current_ad.input_data['call_times']['failed'] / 60)
            # up priority and schedule a visit at most likely time to be in?
            self.priority -= 10

            self.rep.adviser_store.put(current_ad)
            self.arranged_visit = True  # basically has requested a visit so go at optimal time and move to front..

            for co in self.district.district_co:

                if self in co.action_plan:
                    # update action plan...bring to top...if exists
                    co.action_plan.pop(co.action_plan.index(self))
                    co.action_plan = [self] + co.action_plan

    def phone_call_outcome(self, current_ad):

        # digital by this point so just can you convince them to reply?
        outcome_test = self.rnd.uniform(0, 100)
        conversion_dict = self.input_data['conversion_rate'][str(h.current_day(self))]

        if outcome_test <= conversion_dict[h.return_time_key(conversion_dict, self.env.now)]:

            yield self.env.timeout(current_ad.input_data['call_times']['success'] / 60)

            self.rep.output_data['Call_success'].append(call_success(self.rep.reps,
                                                                     self.district.name,
                                                                     self.input_data["LA"],
                                                                     self.input_data["LSOA"],
                                                                     self.digital,
                                                                     self.hh_type,
                                                                     self.hh_id,
                                                                     self.env.now))
            self.resp_planned = True
            self.rep.adviser_store.put(current_ad)
            self.rep.env.process(self.respond(self.delay))

        else:

            yield self.env.timeout(current_ad.input_data['call_times']['failed'] / 60)

            self.rep.output_data['Call_failed'].append(call_failed(self.rep.reps,
                                                                   self.district.name,
                                                                   self.input_data["LA"],
                                                                   self.input_data["LSOA"],
                                                                   self.digital,
                                                                   self.hh_type,
                                                                   self.hh_id,
                                                                   self.env.now))

            self.rep.adviser_store.put(current_ad)

    def respond(self, delay=0):
        """represents the hh responding - not the return being received by census"""

        if self.responded is False:

            self.responded = True
            self.resp_time = self.env.now
            # add to hh response event log

            self.output_data['Respond'].append(response(self.rep.reps,
                                                        self.district.name,
                                                        self.input_data["LA"],
                                                        self.input_data["LSOA"],
                                                        self.digital,
                                                        self.hh_type,
                                                        self.hh_id,
                                                        self.resp_time))

            if self.delay == 0:  # digital
                self.env.process(censusv2.ret_rec(self, self.rep))
            else:  # paper
                start_delayed(self.env, censusv2.ret_rec(self, self.rep), delay)

            yield self.env.timeout((self.rep.sim_hours) - self.env.now)  # hh does no more (without intervention)

    def receive_reminder(self, reminder_type):

        self.status = "received reminder"

        if self.resp_planned and self.responded:

            self.rep.output_data['Reminder_wasted'].append(reminder_wasted(self.rep.reps,
                                                                           self.district.name,
                                                                           self.input_data["LA"],
                                                                           self.input_data["LSOA"],
                                                                           self.digital,
                                                                           self.hh_type,
                                                                           self.hh_id,
                                                                           self.env.now,
                                                                           reminder_type))
        elif self.resp_planned and not self.responded:

            self.rep.output_data['Reminder_unnecessary'].append(reminder_unnecessary(self.rep.reps,
                                                                                     self.district.name,
                                                                                     self.input_data["LA"],
                                                                                     self.input_data["LSOA"],
                                                                                     self.digital,
                                                                                     self.hh_type,
                                                                                     self.hh_id,
                                                                                     self.env.now,
                                                                                     reminder_type))

        elif not self.resp_planned and not self.responded and reminder_type == 'pq' and not self.digital:
            # hh who have chosen not to respond due to not having paper but are now given paper

            self.rep.output_data['Reminder_success'].append(reminder_success(self.rep.reps,
                                                                             self.district.name,
                                                                             self.input_data["LA"],
                                                                             self.input_data["LSOA"],
                                                                             self.digital,
                                                                             self.hh_type,
                                                                             self.hh_id,
                                                                             self.env.now,
                                                                             reminder_type))

            self.resp_level = self.set_behaviour('response')
            self.help_level = self.resp_level + self.set_behaviour('help')
            yield self.env.process(self.action())

        elif not self.resp_planned and not self.responded and reminder_type == 'pq' and self.digital:
                # hh who have chosen not to respond despite happy to use digital - paper makes no/little difference?

                self.resp_level = 0
                self.help_level = 0

        else:
            # some other reminder received - will need to understand the effectiveness of these whatever they are
            self.resp_level = 0
            self.help_level = 0

    def record_wait_time(self, wait_time, renege_time):

        self.output_data['Call_wait_times'].append(call_wait_times(self.rep.reps,
                                                                   self.district.name,
                                                                   self.input_data["LA"],
                                                                   self.input_data["LSOA"],
                                                                   self.digital,
                                                                   self.hh_type,
                                                                   self.hh_id,
                                                                   self.env.now,
                                                                   min(wait_time, renege_time)))

    def receive_letter(self, effect, pq):
        """represents the hh receiving a letter"""

        self.letter_count += 1

        if h.str2bool(pq) and not self.responded:

            # then it's a paper questionnaire so now follow the default behaviour
            self.rep.output_data['Received_pq'].append(received_pq(self.rep.reps,
                                                                   self.district.name,
                                                                   self.input_data["LA"],
                                                                   self.input_data["LSOA"],
                                                                   self.digital,
                                                                   self.hh_type,
                                                                   self.hh_id,
                                                                   self.rep.env.now,
                                                                   self.hh_id))

            self.paper_allowed = True
            self.resp_level = self.set_behaviour('response')
            self.help_level = self.resp_level + self.set_behaviour('help')
            yield self.env.process(self.action())

        elif not h.str2bool(pq) and not self.responded:

            self.rep.output_data['Received_letter'].append(received_letter(self.rep.reps,
                                                                           self.district.name,
                                                                           self.input_data["LA"],
                                                                           self.input_data["LSOA"],
                                                                           self.digital,
                                                                           self.hh_type,
                                                                           self.hh_id,
                                                                           self.rep.env.now))

            self.resp_level = effect
            self.help_level = 0
            yield self.env.process(self.action())

        elif h.str2bool(pq) and self.responded:
            # wasted pq
            self.rep.output_data['Wasted_pq'].append(received_pq(self.rep.reps,
                                                                 self.district.name,
                                                                 self.input_data["LA"],
                                                                 self.input_data["LSOA"],
                                                                 self.digital,
                                                                 self.hh_type,
                                                                 self.hh_id,
                                                                 self.rep.env.now))

        elif not h.str2bool(pq) and self.responded:
            # waster letter
            self.rep.output_data['Wasted_letter'].append(received_pq(self.rep.reps,
                                                                     self.district.name,
                                                                     self.input_data["LA"],
                                                                     self.input_data["LSOA"],
                                                                     self.digital,
                                                                     self.hh_type,
                                                                     self.hh_id,
                                                                     self.rep.env.now))












