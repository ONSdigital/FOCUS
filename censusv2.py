"""Module used to store the classes related to census..."""
import math
import datetime as dt
from collections import namedtuple
import helper as h
from simpy.util import start_delayed

return_times = namedtuple('Returned', ['rep', 'district', 'digital', 'hh_type', 'time'])  # time return received
reminder_sent = namedtuple('Reminder_sent', ['rep', 'Time', 'household',  'hh_type', 'type'])
visit = namedtuple('Visit', ['rep', 'district', 'digital', 'hh_type', 'time', 'hh_id'])
visit_contact = namedtuple('Visit_contact', ['rep', 'district', 'digital', 'hh_type', 'time', 'hh_id'])
visit_out = namedtuple('Visit_out', ['rep', 'district', 'digital', 'hh_type', 'time', 'hh_id'])
visit_wasted = namedtuple('Visit_wasted', ['rep', 'district', 'digital', 'hh_type', 'time', 'hh_id'])
visit_success = namedtuple('Visit_success', ['rep', 'district', 'digital', 'hh_type', 'time', 'hh_id'])
visit_failed = namedtuple('Visit_failed', ['rep', 'district', 'digital', 'hh_type', 'time', 'hh_id'])
visit_convert = namedtuple('Visit_convert', ['rep', 'district', 'digital', 'hh_type', 'time', 'hh_id'])
visit_paper = namedtuple('Visit_paper', ['rep', 'district', 'digital', 'hh_type', 'time', 'hh_id'])
visit_unnecessary = namedtuple('Visit_unnecessary', ['rep', 'district', 'digital', 'hh_type', 'time', 'hh_id'])


# a helper process that creates an instance of a StartFU class and starts it working
def start_fu(env, district):

    StartFU(env, district)
    yield env.timeout(0)


def send_reminder(household, reminder_type):

    household.rep.output_data["Reminder_sent"].append(reminder_sent(household.rep.reps,
                                                                    household.env.now,
                                                                    household.digital,
                                                                    household.hh_type,
                                                                    reminder_type))

    household.env.process(household.receive_reminder(reminder_type))
    yield household.env.timeout(0)


def ret_rec(hh, rep):

    hh.returned = True
    rep.total_returns += 1

    rep.output_data['Return'].append(return_times(rep.reps,
                                                  hh.district.name,
                                                  hh.digital,
                                                  hh.hh_type,
                                                  rep.env.now))

    yield rep.env.timeout(0)


class Adviser(object):
    """Call centre adviser - multitasking"""

    def __init__(self, rep):

        self.rep = rep
        self.rep.env.process(self.add_to_store())

    # method to transfer the adviser to the store ready to be claimed
    def add_to_store(self):

        self.rep.ad_avail.remove(self)
        self.rep.adviser_store.put(self)
        yield self.rep.env.timeout(0)


class StartFU(object):
    """represents the RMT creating the visit lists for the assigned districts and CO"""
    def __init__(self, env, district):

        self.env = env
        self.district = district
        self.households = self.district.households
        self.update = self.district.input_data['RMT_update']

        self.visit_list = []

        self.env.process(self.create_visit_lists())

    def create_visit_lists(self):

        self.visit_list = []

        for household in self.households:
            if (household.returned is False and household.visits < household.input_data['max_visits'] and
                    household.input_data['FU_start_time'] <= self.env.now):

                self.visit_list.append(household)

        self.district.rep.rnd.shuffle(self.visit_list)

        slices = len(self.district.district_co)
        split = math.ceil(len(self.visit_list)/slices)

        for co in self.district.district_co:
            if split > len(self.visit_list):
                action_plan = self.visit_list[:]
                self.visit_list = []
            else:
                action_plan = self.visit_list[:split]
                self.visit_list = self.visit_list[split:]

            action_plan.sort(key=lambda hh: hh.priority, reverse=False)
            co.action_plan = action_plan
        yield self.env.timeout(self.update)
        self.env.process(self.create_visit_lists())


class CensusOfficer(object):
    """represents an individual Census Officer. Each instance can be different"""

    def __init__(self, rep,  env, district, input_data):

        self.rep = rep
        self.env = env
        self.district = district
        self.input_data = input_data

        self.action_plan = []
        self.start_date = dt.datetime.strptime((self.input_data['start_date']), '%Y, %m, %d').date()
        self.end_date = dt.datetime.strptime((self.input_data['end_date']), '%Y, %m, %d').date()
        self.has_paper = h.str2bool(self.input_data['has_paper'] )

        self.env.process(self.co_working_test())

    def co_working_test(self):

        if (self.working() and h.returns_to_date(self.district) < self.district.input_data["trigger"] and
                len(self.action_plan) > 0):

            yield self.env.process(self.fu_visit_contact())

        else:
            yield self.env.timeout(next_available(self))

        self.env.process(self.co_working_test())

    def fu_visit_contact(self):

        household = self.action_plan.pop(0)
        self.rep.output_data['Visit'].append(visit(self.rep.reps,
                                                   self.district.name,
                                                   household.digital,
                                                   household.hh_type,
                                                   self.rep.env.now,
                                                   household.id))

        if household.resp_planned:
            self.rep.output_data['Visit_unnecessary'].append(visit_unnecessary(self.rep.reps,
                                                                               self.district.name,
                                                                               household.digital,
                                                                               household.hh_type,
                                                                               self.rep.env.now,
                                                                               household.id))

        household.visits += 1
        household.priority += 1  # automatically lower the priority of this hh after a visit
        contact_test = self.rep.rnd.uniform(0, 100)
        contact_dict = household.input_data['at_home'][str(h.current_day(self))]

        if contact_test <= contact_dict[h.return_time_key(contact_dict, self.env.now)]:
            #in
            self.rep.output_data['Visit_contact'].append(visit_contact(self.rep.reps,
                                                                       self.district.name,
                                                                       household.digital,
                                                                       household.hh_type,
                                                                       self.env.now,
                                                                       household.id))

            yield self.rep.env.process(self.fu_visit_assist(household))

        elif (contact_test > contact_dict[h.return_time_key(contact_dict, self.env.now)] and
              household.visits == household.input_data['max_visits'] and
              h.str2bool(household.input_data['paper_after_max_visits'])):

            household.paper_allowed = True
            self.schedule_paper_drop(household)

            visit_time = self.input_data["visit_times"]["out_paper"]
            yield self.rep.env.timeout((visit_time/60) + self.district.travel_dist/self.input_data["travel_speed"])

        else:
            # out - add drop of of a note
            self.rep.output_data['Visit_out'].append(visit_out(self.rep.reps,
                                                               self.district.name,
                                                               household.digital,
                                                               household.hh_type,
                                                               self.env.now,
                                                               household.id))

            self.env.process(send_reminder(household, 'postcard'))

            visit_time = self.input_data["visit_times"]["out"]
            yield self.rep.env.timeout((visit_time / 60) + self.district.travel_dist / self.input_data["travel_speed"])

    def fu_visit_assist(self, household):

        da_test = self.rep.rnd.uniform(0, 100)
        da_effectiveness = self.input_data['da_effectiveness'][household.hh_type]

        yield self.env.timeout(self.input_data['visit_times']['query']/60)

        # if digital or have already responded skip straight to the outcome of the visit
        if household.digital or household.responded:
            yield self.rep.env.process(self.fu_visit_outcome(household))

        # if not digital try to persuade them to complete online.
        elif not household.digital and da_test <= da_effectiveness:
            yield self.env.timeout(self.input_data['visit_times']['convert']/60)

            self.rep.output_data['Visit_convert'].append(visit_convert(self.rep.reps,
                                                                       self.district.name,
                                                                       household.digital,
                                                                       household.hh_type,
                                                                       self.rep.env.now,
                                                                       household.id))

            household.digital = True
            yield self.rep.env.process(self.fu_visit_outcome(household))

        # if not digital, do not convince to complete online, and trigger and max visits not reached give paper if on.
        elif (not household.digital and da_test > da_effectiveness and
              h.returns_to_date(self.district) < self.district.input_data['paper_trigger'] and
              household.visits == household.input_data['max_visits'] and
              h.str2bool(household.input_data['paper_after_max_visits'])):

            household.paper_allowed = True
            self.schedule_paper_drop(household)

            visit_time = self.input_data["visit_times"]["paper"]
            yield self.rep.env.timeout((visit_time/60) + self.district.travel_dist/self.input_data["travel_speed"])

        else:
            # or suggest other forms of assistance to be decided...
            # no more - another visit will be scheduled...
            visit_time = self.input_data["visit_times"]["paper"]
            yield self.rep.env.timeout((visit_time / 60) + self.district.travel_dist / self.input_data["travel_speed"])

    def fu_visit_outcome(self, household):

        outcome_test = self.rep.rnd.uniform(0, 100)
        conversion_dict = household.input_data['conversion_rate'][str(h.current_day(self))]

        if household.responded is True:
            self.rep.output_data['Visit_wasted'].append(visit_wasted(self.rep.reps,
                                                                     self.district.name,
                                                                     household.id,
                                                                     household.hh_type,
                                                                     self.env.now,
                                                                     household.id))

            visit_time = self.input_data["visit_times"]["wasted"]
            yield self.rep.env.timeout((visit_time/60) + self.district.travel_dist/self.input_data["travel_speed"])

        # hh have not responded yet and respond there and then either by paper or digital.
        elif (not household.responded and
                outcome_test <= conversion_dict[h.return_time_key(conversion_dict, self.env.now)]):

            self.rep.output_data['Visit_success'].append(visit_success(self.rep.reps,
                                                                       self.district.name,
                                                                       household.id,
                                                                       household.hh_type,
                                                                       self.env.now,
                                                                       household.id))
            household.resp_planned = True
            visit_time = self.input_data["visit_times"]["success"]
            yield self.rep.env.timeout((visit_time/60) + self.district.travel_dist/self.input_data["travel_speed"])
            self.rep.env.process(household.respond(household.delay))

        # hh have not responded but do not respond as a result of the visit.
        elif (not household.responded and
              outcome_test > conversion_dict[h.return_time_key(conversion_dict, self.env.now)] and
              h.returns_to_date(self.district) < self.district.input_data['paper_trigger'] and
              household.visits == household.input_data['max_visits'] and
              h.str2bool(household.input_data['paper_after_max_visits'])):

            self.rep.output_data['Visit_failed'].append(visit_failed(self.rep.reps,
                                                                     self.district.name,
                                                                     household.id,
                                                                     household.hh_type,
                                                                     self.env.now,
                                                                     household.id))
            # leave paper in hope they respond?
            household.paper_allowed = True
            self.schedule_paper_drop(household)

            #if self.has_paper:
                #self.env.process(send_reminder(household, 'pq'))
            #else:
                #start_delayed(self.env, send_reminder(household, 'pq'), h.next_day(self.env.now))

            visit_time = self.input_data["visit_times"]["failed"]
            yield self.rep.env.timeout((visit_time / 60) + self.district.travel_dist/self.input_data["travel_speed"])

    def working(self):
        """returns true or false to depending on whether or not a CO is available at current time"""

        current_date_time = self.rep.start_date + dt.timedelta(hours=self.rep.env.now)
        current_date = current_date_time.date()

        # first check if in correct date range
        if self.start_date <= current_date < self.end_date:

            current_time = current_date_time.time()
            week_day = h.current_day(self)

            avail_data = self.input_data['availability'][str(week_day)]

            for row in avail_data:
                # compare
                if h.make_time(row[0][0], row[0][1], row[0][2]) <= \
                        current_time < h.make_time(row[1][0], row[1][1], row[1][2]):
                    return True

        return False

    def schedule_paper_drop(self, household):

        self.rep.output_data['Visit_paper'].append(visit_paper(self.rep.reps,
                                                               self.district.name,
                                                               household.digital,
                                                               household.hh_type,
                                                               self.rep.env.now,
                                                               household.id))

        if self.has_paper:
            self.env.process(send_reminder(household, 'pq'))
        else:
            start_delayed(self.env, send_reminder(household, 'pq'), h.next_day(self.env.now))


def next_available(co):

    current_date_time = co.rep.start_date + dt.timedelta(hours=co.rep.env.now)
    current_date = current_date_time.date()
    current_time = current_date_time.time()

    # check if correct day.
    if current_date < co.start_date:

        return (co.start_date - current_date).total_seconds()/3600

    # check if there is anyone to visit or if trigger has been reached
    elif len(co.action_plan) == 0 or h.returns_to_date(co.district) >= co.district.input_data["trigger"]:
        return 24 - h.make_time_decimal(current_time)

    # if the right day check if the right time.
    elif co.start_date <= current_date <= co.end_date:
        start_times = co.input_data['availability'][str(co.start_date.weekday())]
        for times in start_times:
            next_start = h.make_time(times[0][0], times[0][1], times[0][2])
            if current_time <= next_start:
                return h.make_time_decimal(next_start) - h.make_time_decimal(current_time)

        # if get to here must be past last start time so wait until next day.
        return 24 - h.make_time_decimal(current_time)

    elif current_date > co.end_date:
        # past last day of work so yield until the end of the simulation.
        return co.rep.sim_hours - co.rep.env.now
