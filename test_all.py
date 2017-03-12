import helper as h
import datetime as dt


def test_str_to_dec():
    assert h.str_to_dec('10:30:00') == 10.5


def test_make_time_decimal():
    assert h.make_time_decimal(dt.datetime.strptime('10:30:00', "%H:%M:%S").time()) == 10.5


def test_str2ool():
    assert h.str2bool("True") == any(["True", "true", "1"])
    assert h.str2bool("true") == any(["True", "true", "1"])
    assert h.str2bool("1") == any(["True", "true", "1"])
    assert h.str2bool("0") != any(["True", "true", "1"])


