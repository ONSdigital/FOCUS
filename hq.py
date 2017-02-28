"""Module used to store the classes related to census..."""
import math
import datetime as dt
from collections import namedtuple
import helper as h
import datetime
from simpy.util import start_delayed
import sys


response_times = namedtuple('Responded', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])  # time Response received
return_received = namedtuple('Return_received', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])
reminder_sent = namedtuple('Reminder_sent', ['rep', 'Time', 'digital',  'hh_type', 'type', 'hh_id'])
visit_paper = namedtuple('Visit_paper', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'time', 'hh_id'])
post_paper = namedtuple('Post_paper', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'time'])
sent_letter = namedtuple('Sent_letter', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'time', 'hh_id'])
warnings = namedtuple('Warnings', ['rep', 'warning', 'detail'])


def send_reminder(household, reminder_type):

    household.rep.output_data["Reminder_sent"].append(reminder_sent(household.rep.reps,
                                                                    household.env.now,
                                                                    household.digital,
                                                                    household.hh_type,
                                                                    reminder_type,
                                                                    household.hh_id))

    start_delayed(household.env, household.receive_reminder(reminder_type), 24)
    yield household.env.timeout(0)


def ret_rec(household, rep):
    # print out every 100000 returns?
    if rep.total_responses % 100000 == 0:
        print(rep.total_responses)

    household.return_received = True
    rep.output_data['Return_received'].append(return_received(rep.reps,
                                                              household.district.name,
                                                              household.la,
                                                              household.lsoa,
                                                              household.digital,
                                                              household.hh_type,
                                                              household.hh_id,
                                                              rep.env.now))
    # currently every return gets counted as a response as soon as it is received - this may need to change
    household.responded = True
    rep.total_responses += 1

    # check size of output data - if over an amount, size or length write to file?
    rep.output_data['Responded'].append(response_times(rep.reps,
                                                       household.district.name,
                                                       household.la,
                                                       household.lsoa,
                                                       household.digital,
                                                       household.hh_type,
                                                       household.hh_id,
                                                       rep.env.now))

    # checks size of output and writes to file if too large
    if (h.dict_size(rep.output_data)) > 1000000:
        h.write_output(rep.output_data, rep.output_path, rep.run)

    yield rep.env.timeout(0)
    # so returned and we know it! remove from simulation??


class Adviser(object):
    """Call centre adviser"""

    def __init__(self, rep, id_num, input_data, type):

        self.rep = rep
        self.id_num = id_num
        self.input_data = input_data
        self.type = type

        # date range in datetime format
        self.start_date = datetime.datetime.strptime(self.input_data['start_date'], '%Y, %m, %d').date()
        self.end_date = datetime.datetime.strptime(self.input_data['end_date'], '%Y, %m, %d').date()

        # date range in simpy format
        self.start_sim_time = h.get_entity_time(self, "start")  # the sim time the adviser starts work
        self.end_sim_time = h.get_entity_time(self, "end")  # the sim time the adviser ends work

        # time range - varies by day
        self.set_avail_sch = input_data['availability']

        # start the processes to add and remove from the store...
        #self.set_availability()

    def set_availability(self):

        start_day = math.floor(self.start_sim_time/24)
        end_day = math.floor(self.end_sim_time/24)

        for i in range(start_day, end_day):

            availability = self.input_data['availability'][str((self.rep.start_date + dt.timedelta(days=i)).weekday())]

            for j in range(0, len(availability), 2):

                in_time = h.make_time_decimal(dt.time(*map(int, availability[j].split(':'))))
                out_time = h.make_time_decimal(dt.time(*map(int, availability[j+1].split(':'))))

                start_time = 24*i + in_time
                end_time = 24*i + out_time

                start_delayed(self.rep.env, self.add_to_store(), start_time)
                start_delayed(self.rep.env, self.remove_from_store(), end_time)

    # method to transfer the adviser to the store ready to be claimed
    def add_to_store(self):

        self.rep.ad_avail.remove(self)
        self.rep.adviser_store.put(self)
        yield self.rep.env.timeout(0)

    def remove_from_store(self):

        current_ad = yield self.rep.adviser_store.get(lambda item: item.id_num == self.id_num)
        self.rep.ad_avail.append(current_ad)
        yield self.rep.env.timeout(0)


class LetterPhase(object):

    def __init__(self, env, rep, district, input_data, letter_type):

        self.env = env
        self.rep = rep
        self.district = district
        self.input_data = input_data
        self.letter_type = letter_type

        self.blanket = h.str2bool(self.input_data["blanket"])
        self.targets = self.input_data["targets"]
        self.start_sim_time = h.get_event_time(self)
        # add process to decide who to send letters too...but with a delay

        start_delayed(self.env, self.fu_letter(), self.start_sim_time)

    def fu_letter(self):

        # send a letter if conditions met
        for household in self.district.households:

            if (not self.blanket and household.hh_type in self.targets and not household.responded) or \
                    (self.blanket and household.hh_type in self.targets):

                # send a letter
                if self.letter_type == 'pq':
                    household.paper_allowed = True

                self.env.process(self.co_send_letter(household,
                                                     self.input_data["delay"]))
        yield self.env.timeout(0)

    def co_send_letter(self, household, delay):

        self.rep.output_data['Sent_letter'].append(sent_letter(self.rep.reps,
                                                               household.district.name,
                                                               household.la,
                                                               household.lsoa,
                                                               household.digital,
                                                               household.hh_type,
                                                               self.rep.env.now,
                                                               household.hh_id))

        #print("letter sent to hh ", household.hh_id)

        yield self.env.timeout(delay)
        self.env.process(household.receive_reminder(self.letter_type))


def schedule_paper_drop(obj, has_paper=False):

    if has_paper:

        obj.rep.output_data['Visit_paper'].append(visit_paper(obj.rep.reps,
                                                              obj.district.name,
                                                              obj.la,
                                                              obj.lsoa,
                                                              obj.digital,
                                                              obj.hh_type,
                                                              obj.rep.env.now,
                                                              obj.hh_id))

        obj.env.process(send_reminder(obj, 'pq'))
    else:

        obj.output_data['Post_paper'].append(post_paper(obj.rep.reps,
                                                        obj.district.name,
                                                        obj.la,
                                                        obj.lsoa,
                                                        obj.digital,
                                                        obj.hh_type,
                                                        obj.env.now))

        start_delayed(obj.env, send_reminder(obj, 'pq'), h.next_day(obj.env.now))


