import time
import psycopg2
import csv
import datetime
from locust import Locust, TaskSet, events, task


class PostgresClient(object):
    def __init__(self, host, dbname, user, password):
        connect_str = "dbname='" + dbname + "' user='" + user + "' host='" + host + "' " + \
                      "password='" + password + "'"
        conn = psycopg2.connect(connect_str)
        self.cursor = conn.cursor()

    def __getattr__(self, name):

        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                self.cursor.execute(*args, **kwargs)
                result = self.cursor.fetchall()
            except Exception as e:
                total_time = int((time.time() - start_time) * 1000)
                events.request_failure.fire(request_type="psycopg2", name=name, response_time=total_time, exception=e)
            else:
                total_time = int((time.time() - start_time) * 1000)
                events.request_success.fire(request_type="psycopg2", name=name, response_time=total_time,
                                            response_length=len(str(result)))
        return wrapper


class PostgresLocust(Locust):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.client = PostgresClient(self.host, self.dbname, self.user, self.password)
        events.request_failure += self.hook_request_fail
        events.quitting += self.hook_locust_quit
        request_failures_file = open("locust_request_failures.csv", "w")
        writer = csv.writer(request_failures_file, quoting=csv.QUOTE_NONNUMERIC)
        writer.writerow(['timestamp', 'name', 'request_type', 'response_time', 'exception'])
        request_failures_file.flush()
        request_failures_file.close()
        self.request_failures_file = open("locust_request_failures.csv", "a")
        self.writer = csv.writer(self.request_failures_file, quoting=csv.QUOTE_NONNUMERIC)

    def hook_request_fail(self, request_type, name, response_time, exception):
        self.writer.writerow([datetime.datetime.now().isoformat(), name, request_type, response_time, exception])

    def hook_locust_quit(self):
        self.request_failures_file.flush()
        self.request_failures_file.close()