import time
import pg8000
import csv
import datetime
from locust import Locust, events
from sqlalchemy import create_engine
from sqlalchemy.engine import url


class PostgresClient(object):
    def __init__(self, host, port, dbname, user, password, readonly=True, autocommit=True,
                 request_type='pg8000', pool_size=1, max_overflow=0):
        self.request_type = request_type
        database_connection_params = {
            'drivername': 'postgres+pg8000',
            'host': host,
            'port': port,
            'username': user,
            'password': password,
            'database': dbname
        }
        self.engine = create_engine(url.URL(**database_connection_params),
                                    pool_size=pool_size, max_overflow=max_overflow,
                                    isolation_level="AUTOCOMMIT"
                                    )

    def __getattr__(self, name):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = self.engine.execute(*args, **kwargs).fetchall()
            except Exception as e:
                total_time = int((time.time() - start_time) * 1000)
                events.request_failure.fire(request_type=self.request_type, name=name, response_time=total_time,
                                            exception=e)
            else:
                total_time = int((time.time() - start_time) * 1000)
                events.request_success.fire(request_type=self.request_type, name=name, response_time=total_time,
                                            response_length=len(str(result)))
        return wrapper


class PostgresLocust(Locust):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.client = PostgresClient(self.host, self.port, self.dbname, self.user, self.password,
                                     pool_size=self.pool_size)
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
