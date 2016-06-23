class TaskTransactionError(Exception):
    pass


class Retry(Exception):
    def __init__(self, eta=None, delay=None, trace=None):
        """
        :type eta: datetime.datetime
        :type delay: datetime.timedelta
        """
        super(Retry, self).__init__()
        self.eta = eta
        self.delay = delay
        self.trace = trace
