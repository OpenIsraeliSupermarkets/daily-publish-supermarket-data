import datetime
import pytz


def now(backfill_hours=0):
    return datetime.datetime.now(pytz.timezone("Asia/Jerusalem")) - datetime.timedelta(hours=backfill_hours)
