from datetime import datetime, timedelta
from typing import Optional


class TaskTransactionError(Exception):
    pass


class Retry(Exception):
    def __init__(self, eta: Optional[datetime] = None, delay: Optional[timedelta] = None,
                 trace: Optional[str] = None) -> None:
        super(Retry, self).__init__()
        self.eta = eta
        self.delay = delay
        self.trace = trace
