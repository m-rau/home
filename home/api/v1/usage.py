from datetime import datetime, timedelta

import pandas as pd
from bokeh.embed import json_item
from bokeh.plotting import figure
from bokeh.resources import CDN
from core4.api.v1.request.main import CoreRequestHandler
from core4.util.node import now


class LoginCountHandler(CoreRequestHandler):
    author = "mra"
    title = "core4 login count"

    async def get(self, mode=None):
        """
        Same as :meth:`.post`
        """
        return await self.post(mode)

    async def post(self, mode=None):
        """
        Retrieve unique user login statistics. The optional ``mode`` operator
        controls the output format. With mode ``raw`` the data is delivered
        with the requested content type (html, csv, text or json).

        Methods:
            POST /usage/login - deliver results

        Parameters:
            start (str): inclusive lower bound
            end (str): exclusive upper bound
            aggregate (chr): daily (d), weekly (w), monthly (m), quarterly (q)
                or yearly (y)
            content_type (str): force json, html, text

        Returns:
            data element with list of aggregation time interval and count of
            unique users who logged in

        Raises:
            401 Unauthorized:

        Examples:
            GET http://devops:5001/usage/login?start=2017-01-01&aggregate=W
            GET http://devops:5001/usage/login/raw?content_type=json
        """
        end = self.get_argument("end", as_type=datetime, default=now())
        start = self.get_argument("start", as_type=datetime,
                                  default=end - timedelta(days=90))
        aggregate = self.get_argument("aggregate", as_type=str,
                                      default="w")
        if mode in ("plot", "raw"):
            df = await self._query(start, end, aggregate)
            if mode == "raw":
                return self.reply(df)
            x = df.timestamp
            y = df.user
            p = figure(title="unique users", x_axis_label='week',
                       sizing_mode="stretch_both", y_axis_label='logins',
                       x_axis_type="datetime")
            p.line(x, y, line_width=4)
            p.title.text = "core usage by users"
            p.title.align = "left"
            p.title.text_font_size = "25px"
            return self.reply(json_item(p, "myplot"))
        return self.render("templates/usage.html",
                           rsc=CDN.render(),
                           start=start,
                           end=end,
                           aggregate=aggregate)

    async def _query(self, start, end, aggregate):
        coll = self.config.home.usage.login.connect_async()
        cur = coll.aggregate([
            {
                "$match": {
                    "_id": {
                        "$gte": start,
                        "$lt": end
                    }
                }
            },
            {
                "$unwind": "$data"
            },
            {
                "$project": {
                    "_id": 0,
                    "user": {"$arrayElemAt": ['$data', 0]},
                    "timestamp": {"$arrayElemAt": ['$data', 1]}
                }
            }
        ])
        data = []
        async for doc in cur:
            data.append(doc)
        df = pd.DataFrame(data).set_index("timestamp")
        g = df.groupby(pd.Grouper(freq=aggregate)).user.nunique()
        return g.sort_index().reset_index()
