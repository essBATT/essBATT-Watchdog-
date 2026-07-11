# This is free and unencumbered software released into the public domain.
# (full license text omitted for brevity - same as original)

"""Utility classes and helper functions for essBATT Watchdog."""

import time
import threading


# Class from MestreLion: https://stackoverflow.com/questions/474528/how-to-repeatedly-execute-a-function-every-x-seconds
class RepeatedTimer(object):
    """Repeatedly executes a function every X seconds using threading.Timer."""

    def __init__(self, interval, function, *args, **kwargs):
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.next_call = time.time()
        self.start()

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            now = time.time()
            if self.next_call < now:
                self.next_call = now
            self.next_call += self.interval
            delay = max(0.01, self.next_call - time.time())
            self._timer = threading.Timer(delay, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        if self._timer:
            self._timer.cancel()
        self.is_running = False
