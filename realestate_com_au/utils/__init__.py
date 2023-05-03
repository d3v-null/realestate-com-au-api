from datetime import datetime
import re


def delete_nulls(obj):
    new_obj = {}
    for key, val in obj.items():
        if val is not None:
            if isinstance(val, dict):
                new_obj[key] = delete_nulls(val)
            else:
                new_obj[key] = val
    return new_obj


def func_or_none(f, a):
    return None if not a else f(a)


def strp_date_or_none(s, fmt):
    if type(fmt) == str:
        fmt = [fmt]
    for f in fmt:
        try:
            return func_or_none(lambda a: datetime.strptime(a, f).date(), s)
        except ValueError:
            pass


def float_or_none(s):
    if type(s) == str:
        s = re.sub(r'[^\d.-]+', '', s)
    return func_or_none(float, s)


def positive_float_or_none(s):
    res = float_or_none(s)
    return None if res is None or res < 0 else res


def positive_int_or_none(s):
    if type(s) == str:
        s = re.sub(r'[^\d]+', '', s)
    res = func_or_none(int, s)
    return None if res is None or res < 0 else res
