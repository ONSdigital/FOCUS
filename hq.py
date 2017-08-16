"""Module used to store the classes and other code related to any aspect of the census hq operation"""

import output_options as oo
import helper as h
import datetime
from simpy.util import start_delayed
import math


def ret_rec(household, rep):
    # print out every 100000 returns?
    if rep.total_responses % 100000 == 0:
        print(rep.total_responses)

    if oo.record_active_summary:
        # add household to summary of responses
        for key, value in rep.active_summary.items():
            value[str(getattr(household, key))][math.floor(rep.env.now / 24)] += 1

        for key, value in rep.active_totals.items():
            value[str(getattr(household, key))] += 1

    if oo.record_active_paper_summary and not household.digital:

        for key, value in rep.active_paper_summary.items():
            value[str(getattr(household, key))][math.floor(rep.env.now / 24)] += 1

        for key, value in rep.active_paper_totals.items():
            value[str(getattr(household, key))] += 1

    household.return_received = True
    if oo.record_return_received:
        rep.output_data['Return_received'].append(oo.generic_output(rep.reps,
                                                                    household.district.district,
                                                                    household.la,
                                                                    household.lsoa,
                                                                    household.digital,
                                                                    household.hh_type,
                                                                    household.hh_id,
                                                                    rep.env.now))
    # currently every return gets counted as a response as soon as it is received - this may need to change
    household.responded = True
    rep.total_responses += 1
    household.district.total_responses += 1

    # check size of output data - if over an amount, size or length write to file?
    if oo.record_responded:
        rep.output_data['Responded'].append(oo.generic_output(rep.reps,
                                                              household.district.district,
                                                              household.la,
                                                              household.lsoa,
                                                              household.digital,
                                                              household.hh_type,
                                                              household.hh_id,
                                                              rep.env.now))

    # checks size of output and writes to file if too large
    if (h.dict_size(rep.output_data)) > rep.max_output_file_size:
        h.write_output(rep.output_data, rep.output_path, rep.run)

    yield rep.env.timeout(0)
    # so returned and we know it! remove from simulation??


class Adviser(object):
    """Call centre adviser"""

    def __init__(self, rep, id_num, input_data, ad_type):

        self.rep = rep
        self.id_num = id_num
        self.input_data = input_data
        self.type = ad_type

        # date range in datetime format
        self.start_date = datetime.datetime.strptime(self.input_data['start_date'], '%Y, %m, %d').date()
        self.end_date = datetime.datetime.strptime(self.input_data['end_date'], '%Y, %m, %d').date()

        # date range in simpy format
        self.start_sim_time = h.get_entity_time(self, "start")  # the sim time the adviser starts work
        self.end_sim_time = h.get_entity_time(self, "end")  # the sim time the adviser ends work

        # time range - varies by day of week
        self.set_avail_sch = input_data['availability']


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

                    if oo.record_paper_summary:
                        # add to the summary of the amount of paper given

                        for key, value in self.rep.paper_summary.items():
                            value[str(getattr(household, key))][math.floor(self.env.now / 24)] += 1

                        for key, value in self.rep.paper_totals.items():
                            value[str(getattr(household, key))] += 1

                self.env.process(self.co_send_letter(household, self.letter_type, self.input_data["delay"]))

        yield self.env.timeout(0)

    def co_send_letter(self, household, letter_type, delay):
        if oo.record_letters:
            self.rep.output_data[letter_type + '_sent'].append(oo.generic_output(self.rep.reps,
                                                                                 household.district.district,
                                                                                 household.la,
                                                                                 household.lsoa,
                                                                                 household.digital,
                                                                                 household.hh_type,
                                                                                 household.hh_id,
                                                                                 self.env.now))

        yield self.env.timeout(delay)
        self.env.process(household.receive_reminder(letter_type))


def schedule_paper_drop(obj, contact_type, reminder_type, delay):

    # add to summary of paper given out
    if reminder_type == 'pq' and oo.record_paper_summary:

        for key, value in obj.rep.paper_summary.items():
            value[str(getattr(obj, key))][math.floor(obj.rep.env.now / 24)] += 1

        for key, value in obj.rep.paper_totals.items():
            value[str(getattr(obj, key))] += 1

    output_type = contact_type + "_" + reminder_type + "_posted"   # use this as output key

    if oo.record_posted:
        obj.rep.output_data[output_type].append(oo.generic_output(obj.rep.reps,
                                                                  obj.district.district,
                                                                  obj.la,
                                                                  obj.lsoa,
                                                                  obj.digital,
                                                                  obj.hh_type,
                                                                  obj.hh_id,
                                                                  obj.env.now))

    if delay > 0:
        start_delayed(obj.env, obj.receive_reminder(reminder_type), delay)
    else:
        obj.env.process(obj.receive_reminder(reminder_type))

    yield obj.env.timeout(0)
