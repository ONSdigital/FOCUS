"""file used to store the classes and definitions related to the households represented in the simulation """

import hq
from simpy.util import start_delayed
import output_options as oo
import helper as h
import math
import datetime as dt
import call_profiles as cp


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
        self.engaged = initial_action.engaged
        self.district_name = district.district

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
        self.time_spent_visits = 0
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
            if oo.record_do_nothing:
                self.output_data['Do_nothing'].append(oo.generic_output(self.rep.reps,
                                                                        self.district.district,
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

        if oo.record_call:
            self.output_data['Call'].append(oo.generic_output(self.rep.reps,
                                                              self.district.district,
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
            # call to ask so may need to up the level of response above default here? Extra optional variable?
            yield self.env.process(hq.schedule_paper_drop(self, 'Call', 'pq', self.district.postal_delay))

        elif isinstance(self.adviser_check(self.env.now), str):
            # otherwise carry on to the call centre to speak to an adviser
            # unless outside times advisers are available - use check times and if none returned gracefully defer
            yield self.env.process(self.phone_call_connect())

        else:
            # no one available - gracefully defer - some will call back again?
            if oo.record_call_defer:
                self.output_data['Call_defer'].append(oo.generic_output(self.rep.reps,
                                                                        self.district.district,
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

    def action_test(self, interaction_type):
        # tests what a household will do next after an interaction and returns the action and time of action.

        test_value = self.rnd.uniform(0, 100)
        # test if default or alt
        behaviour = self.default_behaviour()

        dict_value = self.input_data["behaviours"][interaction_type]

        if test_value <= dict_value["response"]:
            # respond straight away
            return ["respond", self.env.now]
        elif dict_value["response"] < test_value <= dict_value["response"] + dict_value["help"]:
            # or call again sometime later, set at 1 hour later for now...
            return ["contact", 1]
        else:
            return ["Do nothing", 0]

    def adviser_check(self, time):
        # returns the adviser type that is available at the current time if any

        for k, v in self.rep.adviser_types.items():
            # between the dates
            if v['start_time'] < time <= v['end_time']:

                # then between each set of times
                current_dow = str((self.rep.start_date + dt.timedelta(days=math.floor(time / 24))).weekday())
                for i in range(0, len(v['availability'][current_dow]), 2):
                    in_time = h.make_time_decimal(dt.time(*map(int, v['availability'][current_dow][i].split(':'))))
                    out_time = h.make_time_decimal(dt.time(*map(int, v['availability'][current_dow][i+1].split(':'))))

                    if in_time <= self.env.now % 24 < out_time:
                        return k

        return None

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

            if oo.record_call_renege:
                self.output_data['Call_renege'].append(oo.generic_output(self.rep.reps,
                                                                         self.district.district,
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
            if oo.record_call_contact:
                self.output_data['Call_contact'].append(oo.generic_output(self.rep.reps,
                                                                          self.district.district,
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

        if self.digital or self.paper_allowed:
            # no need to persuade go straight to the outcome
            yield self.rep.env.process(self.phone_call_outcome(current_ad))

        elif not self.digital and da_test <= da_effectiveness:
            # not digital and adviser converts household to digital but takes some time
            yield self.env.timeout(current_ad.input_data['call_times']['convert'] / 60)

            # record event
            if oo.record_call_convert:
                self.rep.output_data['Call_convert'].append(oo.generic_output(self.rep.reps,
                                                                              self.district.district,
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

            if oo.record_call_request:
                self.rep.output_data['Call_request'].append(oo.generic_output(self.rep.reps,
                                                                              self.district.district,
                                                                              self.la,
                                                                              self.lsoa,
                                                                              self.digital,
                                                                              self.hh_type,
                                                                              self.hh_id,
                                                                              self.env.now))

            # up priority and tag so a visit happens at time likely time to be in - or just set it to succeed - or both?
            self.priority -= 10

            self.rep.adviser_store.put(current_ad)
            # self.arranged_visit = True  # basically has requested a visit so go at optimal time and move to front..
            self.arranged_visit = False

    def phone_call_outcome(self, current_ad):

        # digital by this point so just can you convince them to reply?
        outcome_test = self.rnd.uniform(0, 100)
        conversion_dict = self.input_data['success_rate'][str(h.current_day(self))]

        if outcome_test <= conversion_dict[h.return_time_key(conversion_dict, self.env.now)]:

            yield self.env.timeout(current_ad.input_data['call_times']['success'] / 60)

            if oo.record_call_success:
                self.rep.output_data['Call_success'].append(oo.generic_output(self.rep.reps,
                                                                              self.district.district,
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

            if oo.record_call_failed:
                self.rep.output_data['Call_failed'].append(oo.generic_output(self.rep.reps,
                                                                             self.district.district,
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
            if oo.record_return_sent:
                self.output_data['Return_sent'].append(oo.generic_output(self.rep.reps,
                                                                         self.district.district,
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

        if oo.record_reminder_received:
            self.rep.output_data[reminder_type + '_received'].append(oo.reminder_received(self.rep.reps,
                                                                                          self.district.district,
                                                                                          self.la,
                                                                                          self.lsoa,
                                                                                          self.digital,
                                                                                          self.hh_type,
                                                                                          self.hh_id,
                                                                                          self.env.now,
                                                                                          reminder_type))
        # set resp according to type of hh and reminder
        if not self.resp_planned and reminder_type == 'pq' and self.engaged:
            self.paper_allowed = True
            self.resp_level = 100  # so this assumes if you provide paper to those engaged they will respond

        elif not self.resp_planned:
            behaviour = self.default_behaviour()
            # and get relevant figures
            try:
                response_data = self.input_data["behaviours"][reminder_type][behaviour]
            except:
                pass
            self.resp_level = response_data["response"]
            if self.rep.total_ad_instances > 0:
                self.help_level = response_data["help"]
            else:
                self.help_level = 0

        # recorded if wasted, unnecessary or successful
        if self.responded:

            if oo.record_reminder_wasted:
                self.rep.output_data[reminder_type + '_wasted'].append(oo.reminder_wasted(self.rep.reps,
                                                                                          self.district.district,
                                                                                          self.la,
                                                                                          self.lsoa,
                                                                                          self.digital,
                                                                                          self.hh_type,
                                                                                          self.hh_id,
                                                                                          self.env.now,
                                                                                          reminder_type))
        elif self.resp_planned:

            if oo.record_reminder_unnecessary:
                self.rep.output_data[reminder_type + '_unnecessary'].append(oo.reminder_unnecessary(self.rep.reps,
                                                                                                    self.district.district,
                                                                                                    self.la,
                                                                                                    self.lsoa,
                                                                                                    self.digital,
                                                                                                    self.hh_type,
                                                                                                    self.hh_id,
                                                                                                    self.env.now,
                                                                                                    reminder_type))
        # now move on to the relevant action based on extracted values
        reminder_test = self.rnd.uniform(0, 100)

        if not self.resp_planned and reminder_test <= self.resp_level:
            if oo.record_reminder_success:
                self.rep.output_data[reminder_type + '_success'].append(oo.reminder_success(self.rep.reps,
                                                                                            self.district.district,
                                                                                            self.la,
                                                                                            self.lsoa,
                                                                                            self.digital,
                                                                                            self.hh_type,
                                                                                            self.hh_id,
                                                                                            self.env.now,
                                                                                            reminder_type))
            # change to a start delayed at appropriate time depending on day....
            delay = h.get_time_of_return(self.env.now, self.rep)
            # yield self.env.process(self.household_returns(self.calc_delay()))

            ##########
            # if this is a pq, but a digital household, calculate if the household retunrs via paper or digital?
            # Or assume use preferred method for now?
            # could use paper first paper prop to set this? so set digital to true or false...
            ##########

            start_delayed(self.env, self.household_returns(self.calc_delay()), delay)
            yield self.env.timeout(0)

        elif not self.resp_planned and (self.resp_level < reminder_test <= self.resp_level + self.help_level):
            # call for help...needs to be based on appropriate distribution...not a hardcoded uniform function!
            # also may not do this if intend to respond?
            yield self.env.timeout(self.rnd.uniform(0, 8))

            if oo.record_do_nothing:
                self.output_data[reminder_type + '_contact'].append(oo.generic_output(self.rep.reps,
                                                                                     self.district.district,
                                                                                     self.la,
                                                                                     self.lsoa,
                                                                                     self.digital,
                                                                                     self.hh_type,
                                                                                     self.hh_id,
                                                                                     self.env.now))

            yield self.env.process(self.contact())
        else:
            # nowt
            if oo.record_do_nothing:
                self.output_data[reminder_type + '_failed'].append(oo.generic_output(self.rep.reps,
                                                                                     self.district.district,
                                                                                     self.la,
                                                                                     self.lsoa,
                                                                                     self.digital,
                                                                                     self.hh_type,
                                                                                     self.hh_id,
                                                                                     self.env.now))

        yield self.env.timeout(0)

    def record_wait_time(self, wait_time, renege_time):

        if oo.record_call_wait_times:
            self.output_data['Call_wait_times'].append(oo.call_wait_times(self.rep.reps,
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

