import threading
import time

from thrift_ps import constants


class ModelMonitor(threading.Thread):

    def __init__(self, __ps_handler, __interval, __expired_time):
        threading.Thread.__init__(self)
        self.ps_handler = __ps_handler
        self.interval = __interval
        self.expired_time = __expired_time

    def run(self):
        print("start monitoring ps handler")
        counter = 0
        while counter < constants.MONITOR_COUNTER:
            self.ps_handler.delete_expired(time.time() - self.expired_time)
            time.sleep(self.interval)
            counter += 1
