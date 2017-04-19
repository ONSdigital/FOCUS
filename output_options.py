"""store the named tuples used to record output as well as the values of the labels that defines what sorts of
output are recorded. The more output that is selected the slower the code will run"""

from collections import namedtuple

generic_output = namedtuple('Generic_output', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time'])
reminder_wasted = namedtuple('Reminder_wasted', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time','type'])
reminder_unnecessary = namedtuple('Reminder_unnecessary', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time', 'type'])
reminder_success = namedtuple('Reminder_success', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time', 'type'])
reminder_received = namedtuple('Reminder_received', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time', 'type'])
call_wait_times = namedtuple('Call_wait_times', ['rep', 'district', 'LA', 'LSOA', 'digital', 'hh_type', 'hh_id', 'time', 'wait_time'])
hh_record = namedtuple('hh_record', ['district', 'LA', 'LSOA', 'hh_type', 'action', 'digital', 'action_time'])
warnings = namedtuple('Warnings', ['rep', 'warning', 'detail'])
initial_action = namedtuple('Initial_action', ['type', 'digital', 'time'])
hh_geography = namedtuple('hh_geography', ['la', 'lsoa'])

record_generic_output = True
record_do_nothing = True
record_call = True
record_call_defer = True
record_call_renege = True
record_call_contact = True
record_call_convert = True
record_call_request = True
record_call_success = True
record_call_failed = True
record_return_sent = True
record_reminder_wasted = True
record_reminder_unnecessary = True
record_reminder_received = True
record_reminder_success = True
record_call_wait_times = True
record_warnings = True
record_non_response = True
record_letters = True
record_posted = True
record_visit = True
record_visit_wasted = True
record_visit_unnecessary = True
record_visit_contact = True
record_visit_out = True
record_visit_convert = True
record_visit_assist = True
record_visit_success = True
record_visit_failed = True
record_return_received = True
record_responded = True

