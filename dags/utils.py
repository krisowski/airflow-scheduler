from datetime import datetime
import pytz


def convert_pendulum_object_to_datetime(pendulum_object):
    return datetime(
        year=pendulum_object.year,
        month=pendulum_object.month,
        day=pendulum_object.day,
        hour=pendulum_object.hour,
        minute=pendulum_object.minute,
        second=pendulum_object.second,
        microsecond=pendulum_object.microsecond,
        tzinfo=pendulum_object.tzinfo,
        fold=pendulum_object.fold
    ).replace(tzinfo=pytz.utc)
