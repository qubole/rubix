import logging
from os import listdir
from os.path import isfile, join
from qds_sdk.qubole import Qubole
import signal
import sys
from threading import Event
import time
import random

import workload_runner

class RubixStressor():
    FACT_TABLE_TEMPLATE = "%FACT_TABLE%"
    QUERIES_DIRECTORY = "queries"

    # To be made user inputs
    NUMBER_OF_THREADS = 8
    API_TOKEN = "<QDS_AUTH_TOKEN>"
    CLUSTER_LABEL = "rubix-stress"
    SILENCE_PERIOD_BOUNDARY = 1800
    SILENCE_PERIOD = 180

    fact_tables = []
    queries = []    # list of Pairs of <QueryName, QueryString>
    log = logging.getLogger(__name__)

    # Runtime state
    runners = []
    exitEvent = Event()
    silencePeriodEvent = Event()

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit)
        # For non api.qubole env, set the env in configure()
        Qubole.configure(api_token=self.API_TOKEN)

    def find_fact_tables(self):
        for line in open("fact_tables.sql").readlines():
            if 'CREATE EXTERNAL TABLE IF NOT EXISTS'  in line.upper():
                tableNameStartIdx = line.upper().index("CREATE EXTERNAL TABLE IF NOT EXISTS ") + len("CREATE EXTERNAL TABLE IF NOT EXISTS ")
                tableName = line[tableNameStartIdx:].strip()
                if '(' in tableName:
                    tableName = tableName.split('(')[0].strip()

                self.fact_tables.append(tableName)

    def create_queries(self):
        if len(self.fact_tables) == 0:
            self.find_fact_tables()

        queryFiles = [f for f in listdir(self.QUERIES_DIRECTORY) if isfile(join(self.QUERIES_DIRECTORY, f))]
        for queryFile in queryFiles:
            filepath = join(self.QUERIES_DIRECTORY, queryFile)
            template = " ".join(open(filepath).readlines())
            repetitions = 1
            # For max_all query, add more repetitions to pick it up more than the others because this query gives highest read with least CPU usage
            if "max_all.sql" in queryFile:
                repetitions = 5

            for i in range(0, repetitions):
                for fact_table in self.fact_tables:
                    if (("500" in fact_table) or ("1000" in fact_table) or ("3000" in fact_table) or ("10000" in fact_table)) and ("max_all.sql" not in queryFile):
                        # For bigger scales, only create max_all query, skip tpcds queries
                        continue
                    query = template.replace(self.FACT_TABLE_TEMPLATE, fact_table)
                    self.queries.append([queryFile, query])

    def start(self):
        self.create_queries()

        self.log.warning("Starting threads")
        for i in range(0, self.NUMBER_OF_THREADS):
            runner = workload_runner.WorkloadRunner(self.exitEvent, self.silencePeriodEvent, i, self.queries, self.CLUSTER_LABEL)
            self.runners.append(runner)
            runner.start()

        # Keep triggering silence period at the same time in all threads to ensure downscaling
        time_since_last_break = 0
        while not self.exitEvent.is_set():
            loop_start = time.time()
            # add some randomness in silence period too
            if time_since_last_break >= random.randint(self.SILENCE_PERIOD_BOUNDARY - 600, self.SILENCE_PERIOD_BOUNDARY + 600):
                self.log.warning("Starting silence period")
                self.silencePeriodEvent.set()
                for runner in self.runners:
                    runner.kill_ongoing_commmand()
                start = time.time()
                while ((time.time() - start) < self.SILENCE_PERIOD) and not self.exitEvent.is_set():
                    self.exitEvent.wait(10)
                self.log.warning("Ending silence period")
                self.silencePeriodEvent.clear()
                time_since_last_break = 0
            else:
                self.exitEvent.wait(10)
                time_since_last_break += time.time() - loop_start

    def exit(self, signal, frame):
        self.log.warning("Exiting...")
        self.exitEvent.set()
        for runner in self.runners:
            runner.interrupt()
            runner.log_failures()


def main():
    RubixStressor().start()


if __name__ == '__main__':
    # Use higher level WARN otherwise qds-sdk pollutes the log
    logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(message)s')
    main()
