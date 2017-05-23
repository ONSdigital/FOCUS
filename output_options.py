"""store the named tuples used to record output as well as the values of the labels that defines what sorts of
output are recorded. The more output that is selected the slower the code will run"""

from collections import namedtuple

generic_output = namedtuple('Generic_output', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])
reminder_wasted = namedtuple('Reminder_wasted', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time','type'])
reminder_unnecessary = namedtuple('Reminder_unnecessary', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time', 'type'])
reminder_success = namedtuple('Reminder_success', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time', 'type'])
reminder_received = namedtuple('Reminder_received', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time', 'type'])
call_wait_times = namedtuple('Call_wait_times', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time', 'wait_time'])
hh_record = namedtuple('hh_record', ['rep', 'district', 'LA', 'LSOA', 'hh_type', 'action', 'digital', 'time'])
warnings = namedtuple('Warnings', ['rep', 'warning', 'detail'])
initial_action = namedtuple('Initial_action', ['type', 'digital', 'time'])
hh_geography = namedtuple('hh_geography', ['la', 'lsoa', 'district', 'hh_type', 'digital'])

record_generic_output = False
record_do_nothing = False
record_call = False
record_call_defer = False
record_call_renege = False
record_call_contact = False
record_call_convert = False
record_call_request = False
record_call_success = False
record_call_failed = False
record_return_sent = False
record_reminder_wasted = False
record_reminder_unnecessary = False
record_reminder_received = False
record_reminder_success = False
record_call_wait_times = False
record_warnings = False
record_non_response = False
record_letters = False
record_posted = False
record_visit = False
record_visit_wasted = False
record_visit_unnecessary = False
record_visit_contact = False
record_visit_out = False
record_visit_convert = False
record_visit_assist = False
record_visit_success = False
record_visit_failed = False
record_return_received = False
record_responded = False


