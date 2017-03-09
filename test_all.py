import helper as h


def test_str_to_dec():
    assert h.str_to_dec('10:30:00') == 10.5
