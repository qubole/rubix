import logging
import random
import time
import threading
from threading import Event


from patched_commands import PrestoCommand


class WorkloadRunner(threading.Thread):
    log = logging.getLogger(__name__)

    def __init__(self, exitEvent, silencePeriodEvent, tid, queries, cluster_label):
        threading.Thread.__init__(self)
        self.tid = "thread-" + str(tid)
        self.queries = queries
        self.cluster_label = cluster_label

        self.exitEvent = exitEvent
        self.silencePeriodEvent = silencePeriodEvent

        self.failures = []
        self.backOffTime = 0 # This serves as wait time in case silence period is selected

        self.current_cmd = None

    # Kill ongoing command to start silence period
    def kill_ongoing_commmand(self):
        if self.current_cmd != None:
            # TODO add synchronization to assure 100% cancellations
            self.log.warning("Thread %s Cancelling %s" % (self.tid, self.current_cmd.id))
            self.current_cmd.cancel()

    # Kill ongoing command for exit
    def interrupt(self):
        self.log.warning("Thread %s interrupted" %self.tid)
        self.kill_ongoing_commmand()

    # Sleep when main thread has started silence period
    # Or, Randomly backoff for some time to get random downscaling events
    # Or, Run some query
    def run(self):
        while not self.exitEvent.is_set():
            while self.silencePeriodEvent.is_set():
                self.exitEvent.wait(5)

            if self.exitEvent.is_set():
                return

            should_wait = random.choice([True, False, True, False, False, False])
            if (should_wait) and self.backOffTime != 0:
                # If decided to backoff then backoff between [10s, 120s]
                timeToBackOff = min(max(10, self.backOffTime), 120)
                self.log.warning("Thread %s backing off for %dseconds" %(self.tid, timeToBackOff))
                self.exitEvent.wait(timeToBackOff)
                # reset backOffTime to avoid back to back backOffs
                self.backOffTime = 0
            else:
                self.run_query()

    # Run a query randomly selected from the query pool
    # Sometimes cancel the submitted query after some random time
    # Collect failures
    def run_query(self):
        idx = random.randint(0, len(self.queries) - 1)
        queryName = self.queries[idx][0]
        queryString = self.queries[idx][1]
        start = time.time()

        shouldCancelQuery = random.randint(0, 500) < 25 # cancel with very less chance
        cancelTime = random.randint(10, 500) # lot of times query will finish before this time, so there will be even fewer cancellations
        queryStartTime = time.time()
        self.current_cmd = PrestoCommand.create(name="%s_%s" %(self.tid, queryName),
                                label=self.cluster_label,
                                query=queryString)
        self.log.warning("Thread %s running query %s via Command %s" % (self.tid, queryName, self.current_cmd.id))
        while not self.current_cmd.is_done(self.current_cmd.status):
            if shouldCancelQuery and (time.time() - queryStartTime) > cancelTime:
                self.current_cmd.cancel()
                self.log.warning("Thread %s cancelled Command %s" % (self.tid, self.current_cmd.id))
            self.exitEvent.wait(1)
            self.current_cmd = self.current_cmd.find(self.current_cmd.id)

        elapsed = time.time() - start
        if self.current_cmd.status == "cancelled":
            # expected
            pass
        elif not self.current_cmd.is_success(self.current_cmd.status):
            # TODO: get actual error code and classify failures as per codes
            self.failures.append(self.current_cmd.id)
            self.log.warning("Thread %s Command failed %s" %(self.tid, self.current_cmd.id))
        else:
            self.backOffTime = elapsed
        self.current_cmd = None

    def log_failures(self):
        if len(self.failures) == 0:
            return

        message = "Failures in " + self.tid + ":\n"
        for failure in self.failures:
            message = message + str(failure) + "\n"
        message += "\n"
        self.log.warning(message)

