"""Module used to store the classes related to census..."""
import math
import datetime as dt
from collections import namedtuple

return_times = namedtuple('Returned', ['reps', 'District', 'hh_id', 'Type', 'Time'])  # time full response received
enu_util = namedtuple('Enu_util', ['reps', 'Time', 'Count'])  # enumerator usage over time


# a helper process that creates an instance of a coordinator class and starts it working
def start_fu(env, district):

    StartFU(env, district)
    yield env.timeout(0)


# a simple event representing the response being received
def ret_rec(hh, rep):

    hh.resp_rec = True
    rep.total_returns += 1

    rep.output_data.append(return_times(rep.reps,
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
            action_plan = []
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
        self.district.co_not_working.append(self)

    def contact(self):

        # first test if CO available for work...and if the trigger for the district has been reached?
        if self.working_test() is True:

            if len(self.action_plan) > 0:

                self.co_util_add()
                current_hh = self.action_plan.pop(0)
                yield self.env.timeout(0.5)
                self.co_util_remove()


            else:
                # no one to visit...stop working and wait until the start of the next day...whenever that is
                yield self.env.timeout(24 - (self.env.now/24 - int(self.env.now/24))*24)
        else:
            # the yield time here needs to be until the next time they are due to be available
            yield self.env.timeout(next_available(self))

        self.env.process(self.contact())

    def working_test(self):
        """returns true or false to depending on whether or not a CO is available"""

        current_date_time = self.rep.start_date + dt.timedelta(hours=self.rep.env.now)
        current_date = current_date_time.date()

        # first check if in correct date range
        if self.start_date <= current_date < self.end_date:

            current_time = current_date_time.time()
            current_day = current_date.weekday()

            avail_data = self.input_data['availability_dict'][str(current_day)]

            for row in avail_data:
                # compare
                if make_time(row[0][0], row[0][1], row[0][2]) <= current_time < make_time(row[1][0], row[1][1], row[1][2]):
                    return True

        return False

    def co_util_add(self):

        self.rep.output_data.append(enu_util(self.rep.reps,
                                             self.env.now,
                                             len(self.district.co_working)/len(self.district.district_co)))
        self.district.co_working.append(self)
        self.rep.output_data.append(enu_util(self.rep.reps,
                                             self.env.now,
                                             len(self.district.co_working)/len(self.district.district_co)))

    def co_util_remove(self):

        self.rep.output_data.append(enu_util(self.rep.reps,
                                             self.env.now,
                                             len(self.district.co_working)/len(self.district.district_co)))
        self.district.co_working.remove(self)
        self.rep.output_data.append(enu_util(self.rep.reps,
                                             self.env.now,
                                             len(self.district.co_working)/len(self.district.district_co)))



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

        start_hours = co.input_data['availability_dict'][str(co.start_date.weekday())][0][0][0]
        start_mins = co.input_data['availability_dict'][str(co.start_date.weekday())][0][0][1]
        start_secs = co.input_data['availability_dict'][str(co.start_date.weekday())][0][0][2]
        start_time = make_time_decimal(make_time(start_hours, start_mins, start_secs))

        if current_date < co.start_date:
            # then wait until the next day they are working plus time they start that day
            return (co.start_date - current_date).total_seconds()/3600 + start_time

        elif co.start_date <= current_date <= co.end_date:
            # working day...
            # if working day...get time and see if less than any start times for that day
            start_times = co.input_data['availability_dict'][str(co.start_date.weekday())]
            for times in start_times:
                next_start = make_time(times[0][0], times[0][1], times[0][2])
                if current_time <= next_start:
                    # then yield till then
                    return make_time_decimal(next_start) - make_time_decimal(current_time)

            # if get to here must be passed last start time so wait until next day
            return 24 - make_time_decimal(current_time)







        elif current_date > co.end_date:
            # yield till end of sim
            return co.rep.sim_hours - co.rep.env.now












