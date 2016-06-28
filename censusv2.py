"""Module used to store the classes related to census..."""
import math
import datetime as dt
from collections import namedtuple

return_times = namedtuple('Returned', ['reps', 'District', 'hh_id', 'Type', 'Time'])  # time full response received
visit = namedtuple('Visit', ['reps', 'District', 'Household', 'Type', 'Time'])
visit_contact = namedtuple('Visit_contact', ['run', 'reps', 'Time', 'Household', 'Type'])
visit_out = namedtuple('Visit_out', ['run', 'reps', 'Time', 'Household', 'Type'])
visit_wasted = namedtuple('Visit_wasted', ['run', 'reps', 'Time', 'Household', 'Type'])
visit_success = namedtuple('Visit_success', ['run', 'reps', 'Time', 'Household', 'Type'])


# a helper process that creates an instance of a coordinator class and starts it working
def start_fu(env, district):

    StartFU(env, district)
    yield env.timeout(0)


# a simple event representing the response being received
def ret_rec(hh, rep):

    hh.resp_rec = True
    rep.total_returns += 1

    rep.output_data['Return'].append(return_times(rep.reps,
                                                  hh.district.name,
                                                  hh.hh_id,
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
    """represents the RMT creating the visit lists for the assigned districts"""
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
            if household.resp_rec is False:
                self.visit_list.append(household)

        # now pass those hh to each CO
        slices = len(self.district.district_co)
        split = math.ceil(len(self.visit_list)/slices)

        for co in self.district.district_co:
            if split > len(self.visit_list):
                action_plan = self.visit_list[:]
                self.visit_list = []
            else:
                action_plan = self.visit_list[:split]
                self.visit_list = self.visit_list[split:]

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

        self.env.process(self.contact())

    def contact(self):

        if (self.working() and returns_to_date(self.district) < self.district.input_data["trigger"] and
                len(self.action_plan) > 0):

            current_hh = self.action_plan.pop(0)
            self.rep.output_data['Visit'].append(visit(self.rep.reps,
                                                       self.district.name,
                                                       current_hh.hh_id,
                                                       current_hh.hh_type,
                                                       self.rep.env.now))

            current_hh.visits += 1
            contact_test = self.rep.rnd.uniform(0, 100)

            contact_dict = current_hh.input_data['at_home'][str(current_day(self))]

            if contact_test <= contact_dict[return_time_key(contact_dict, self.env.now)]:

                self.rep.output_data['Visit_contact'].append(visit_contact(self.rep.run,
                                                                           self.rep.reps,
                                                                           self.env.now,
                                                                           current_hh.hh_id,
                                                                           current_hh.hh_type))

                yield self.rep.env.process(self.fu_visit_outcome(current_hh))

            else:
                # out
                self.rep.output_data['Visit_out'].append(visit_out(self.rep.run,
                                                                   self.rep.reps,
                                                                   self.env.now,
                                                                   current_hh.hh_id,
                                                                   current_hh.hh_type))

                yield self.rep.env.timeout((3 / 60))  # need to add travel time calcs

        else:
            # either not at work, no one to visit or trigger reached - check when co next becomes available.
            yield self.env.timeout(next_available(self))

        self.env.process(self.contact())

    def fu_visit_outcome(self, current_hh):

        outcome_test = self.rep.rnd.uniform(0, 100)
        conversion_dict = current_hh.input_data['conversion_rate'][str(current_day(self))]

        if current_hh.resp_sent is True:
            self.rep.output_data['Visit_wasted'].append(visit_wasted(self.rep.run,
                                                                     self.rep.reps,
                                                                     self.rep.env.now,
                                                                     current_hh.hh_id,
                                                                     current_hh.hh_type))

            yield self.rep.env.timeout((5 / 60))  # need to add travel time

        # hh have not responded yet and respond there and then either by paper or digital.
        elif (current_hh.resp_sent is False and
                outcome_test <= conversion_dict[return_time_key(conversion_dict, self.env.now)]):

            self.rep.output_data['Visit_success'].append(visit_success(self.rep.run,
                                                                       self.rep.reps,
                                                                       self.rep.env.now,
                                                                       current_hh.hh_id,
                                                                       current_hh.hh_type))
            current_hh.resp_planned = True
            yield self.rep.env.timeout((30 / 60))  # add travel time
            self.rep.env.process(current_hh.respond(current_hh.delay))

        # hh have not responded but do not respond as a result of the visit.
        elif (current_hh.resp_sent is False and
                outcome_test > conversion_dict[return_time_key(conversion_dict, self.env.now)]):

            yield self.rep.env.timeout((5 / 60))  # add travel time
            # add a link back to hh action with updated behaviours

    def working(self):
        """returns true or false to depending on whether or not a CO is available at current time"""

        current_date_time = self.rep.start_date + dt.timedelta(hours=self.rep.env.now)
        current_date = current_date_time.date()

        # first check if in correct date range
        if self.start_date <= current_date < self.end_date:

            current_time = current_date_time.time()
            week_day = current_day(self)

            avail_data = self.input_data['availability'][str(week_day)]

            for row in avail_data:
                # compare
                if make_time(row[0][0], row[0][1], row[0][2]) <= \
                        current_time < make_time(row[1][0], row[1][1], row[1][2]):
                    return True

        return False


def current_day(obj):

    current_date_time = obj.rep.start_date + dt.timedelta(hours=obj.rep.env.now)
    current_date = current_date_time.date()
    day = current_date.weekday()

    return day


def simpy_to_time(simpy_time):

    days = int(simpy_time/24)
    hours = int(simpy_time - days*24)
    mins = ((simpy_time - days*24) - hours)*60
    secs = int((mins - int(mins))*60)

    time = str(hours) + "," + str(int(mins)) + "," + str(secs)

    return dt.datetime.strptime(time, '%H,%M,%S').time()

def make_time(hours, mins, secs):

    time = str(hours) + "," + str(mins) + "," + str(secs)

    return dt.datetime.strptime(time, '%H,%M,%S').time()


def make_time_decimal(time_object):

    hours = time_object.hour
    mins = time_object.minute
    secs = time_object.second

    return hours + mins/60 + secs/3600


def next_available(co):

    current_date_time = co.rep.start_date + dt.timedelta(hours=co.rep.env.now)
    current_date = current_date_time.date()
    current_time = current_date_time.time()

    # check if correct day.
    if current_date < co.start_date:

        return (co.start_date - current_date).total_seconds()/3600

    # check if there is anyone to visit or if trigger has been reached
    elif len(co.action_plan) == 0 or returns_to_date(co.district) >= co.district.input_data["trigger"]:
        return 24 - make_time_decimal(current_time)

    # if the right day check if the right time.
    elif co.start_date <= current_date <= co.end_date:
        start_times = co.input_data['availability'][str(co.start_date.weekday())]
        for times in start_times:
            next_start = make_time(times[0][0], times[0][1], times[0][2])
            if current_time <= next_start:
                return make_time_decimal(next_start) - make_time_decimal(current_time)

        # if get to here must be past last start time so wait until next day.
        return 24 - make_time_decimal(current_time)

    elif current_date > co.end_date:
        # past last day of work so yield until the end of the simulation.
        return co.rep.sim_hours - co.rep.env.now


def returns_to_date(district):

    count = len([hh.hh_id for hh in district.households if hh.resp_rec is True])

    return (count/len(district.households))*100


def return_time_key(input_dict, time):

    time = make_time_decimal(simpy_to_time(time))

    key_list = sorted(list(input_dict.keys()), key=int)

    for key in key_list:
        if int(key) >= time:
            return key









