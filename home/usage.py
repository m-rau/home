import re
from datetime import timedelta, datetime

import pandas as pd
from core4.queue.helper.job.base import CoreLoadJob
from core4.util.node import now
from dateutil.parser import parse

parse_date = lambda s: None if s is None else parse(s)


class AggregateCore4Usage(CoreLoadJob):
    """
    Reads ``sys.log`` as defined by project configuration key
    ``home.usage.sys_log`` and aggregates all user login logging messages into
    collection ``home.login``. Uses job cookie ``offset`` for
    checkpoint/restart.

    This job should is scheduled daily.
    """
    author = "mra"
    schedule = "30 1 * * *"

    def initialise_object(self):
        self.source_collection = self.config.home.usage.sys_log
        self.target_collection = self.config.home.usage.login

    def get_start(self, start, reset):
        if start is None:
            offset = self.cookie.get("offset")
            if reset or offset is None:
                return self.config.home.usage.start
            return offset
        return parse_date(start)

    def execute(self, start=None, end=None, reset=False, **kwargs):
        start = self.get_start(start, reset)
        end = parse_date(end) or now()
        start = start.date()
        end = end.date()
        if end < start or end > now().date():
            raise RuntimeError("unexpected date range [{} - {}]".format(
                start, end
            ))
        ndays = (end - start).days + 1.
        self.logger.info("scope [%s] (%s) - [%s] (%s) = [%d] days",
                         start, type(start), end, type(end), ndays)
        n = 0
        while start <= end:
            n += 1.
            self.progress(n / ndays, "work [%s] day [%d]", start, n)
            self.reset(start)
            self.extract(start)
            start += timedelta(days=1)
        self.cookie.set(offset=datetime.combine(end, datetime.min.time()))

    def reset(self, start):
        start = datetime.combine(start, datetime.min.time())
        ret = self.target_collection.delete_one({"date": start})
        self.logger.debug("removed [%d] records with [%s]", ret.deleted_count,
                          start)

    def extract(self, start):
        end = start + timedelta(days=1)
        start = datetime.combine(start, datetime.min.time())
        end = datetime.combine(end, datetime.min.time())
        cur = self.source_collection.find(
            {
                "created": {
                    "$gte": start,
                    "$lt": end
                },
                "message": re.compile("successful login"),
                "user": {
                    "$ne": "admin"
                }
            },
            sort=[("_id", -1)],
            projection=["created", "user"]
        )
        data = list(cur)
        self.logger.debug("extracted [%d] records in [%s] - [%s]", len(data),
                          start, end)
        if data:
            self.set_source(str(start.date()))
            self.target_collection.update_one(
                filter={"_id": start},
                update={
                    "$set": {
                        "data": [(d["user"], d["created"]) for d in data]
                    }
                },
                upsert=True)


if __name__ == '__main__':
    from core4.queue.helper.functool import execute
    execute(AggregateCore4Usage, reset=True)
