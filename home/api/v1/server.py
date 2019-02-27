from core4.api.v1.application import CoreApiContainer
from home.api.v1.usage import LoginCountHandler


class UsageServer(CoreApiContainer):
    """
    API/widget server delivering unique user login statistics and chart.
    """
    root = "/usage"
    rules = [
        (r'/login', LoginCountHandler),
        (r'/login/(.+)', LoginCountHandler),
    ]


if __name__ == '__main__':
    from core4.api.v1.tool.functool import serve
    serve(UsageServer)
