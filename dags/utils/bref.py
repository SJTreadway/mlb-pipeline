import datetime
from time import sleep
from typing import Any, Optional

from curl_cffi import requests

from typing import Any, Optional, Type

class Singleton:
    # Use this and __new__ to make this a singleton. Only one ever exists.
    __INSTANCE__: Optional['Singleton'] = None
    def __new__(cls: Type['Singleton'], *args: Any, **kwargs: Any) -> 'Singleton':
        if cls.__INSTANCE__ == None:
            cls.__INSTANCE__ = super(Singleton, cls).__new__(cls)

        assert cls.__INSTANCE__ is not None
        return cls.__INSTANCE__ 

class BRefSession(Singleton):
    """
    This is needed because Baseball Reference has rules against bots.

    Current policy says no more than 20 requests per minute, but in testing
    anything more than 10 requests per minute gets you blocked for one hour.

    So this global session will prevent a user from getting themselves blocked.
    """

    def __init__(self, max_requests_per_minute: int = 10) -> None:
        self.max_requests_per_minute = max_requests_per_minute
        self.last_request: Optional[datetime.datetime]  = None
        self.session = requests.Session()
    
    def get(self, url: str, **kwargs: Any) -> requests.Response:
        if self.last_request:
            delta = datetime.datetime.now() - self.last_request
            sleep_length = (60 / self.max_requests_per_minute) - delta.total_seconds()
            if sleep_length > 0:
                sleep(sleep_length)

        self.last_request = datetime.datetime.now()
        try:
            resp = self.session.get(url, impersonate="chrome", **kwargs)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")

        return -1
                