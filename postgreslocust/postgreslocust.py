import time
import csv
import datetime
from locust import Locust, events
from sqlalchemy import create_engine
from sqlalchemy.engine import url
from sqlalchemy import event
from argparse import ArgumentParser


class PostgresClient(object):
    def __init__(self, host, port, dbname, user, password, redshift_cache_query_results=False,
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

        # Execute Redshift command to disable session level query caching for each new connection created
        if not redshift_cache_query_results:
            event.listen(self.engine, 'connect', self.disable_result_cache_for_session)

    @staticmethod
    def disable_result_cache_for_session(dbapi_con, connection_record):
        cursor = dbapi_con.cursor()
        cursor.execute('SET enable_result_cache_for_session TO OFF;')

    def __getattr__(self, name):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = self.engine.execute(*args, **kwargs).fetchall()
            except Exception as e:
                print('Exception occurred: ' + str(e))
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

        # replace empty configuration with default value
        if not hasattr(self, 'redshift_cache_query_results'):
            self.redshift_cache_query_results = True

        self.client = PostgresClient(self.host, self.port, self.dbname, self.user, self.password,
                                     pool_size=self.pool_size,
                                     redshift_cache_query_results=self.redshift_cache_query_results)
        events.request_failure += self.hook_request_fail
        events.quitting += self.hook_locust_quit

        parser = ArgumentParser()
        parser.add_argument("--csv", dest="log_file_prefix")
        args, unknown = parser.parse_known_args()
        print('log_file_prefix: ' + args.log_file_prefix)
        failures_file_name = args.log_file_prefix + '_failures.csv'

        request_failures_file = open(failures_file_name, "w")
        writer = csv.writer(request_failures_file, quoting=csv.QUOTE_NONNUMERIC, lineterminator='\n')
        writer.writerow(['timestamp', 'name', 'request_type', 'response_time', 'exception'])
        request_failures_file.flush()
        request_failures_file.close()
        self.request_failures_file = open(failures_file_name, "a")
        self.writer = csv.writer(self.request_failures_file, quoting=csv.QUOTE_NONNUMERIC, lineterminator='\n')

    def hook_request_fail(self, request_type, name, response_time, exception):
        self.writer.writerow([datetime.datetime.now().isoformat(), name, request_type, response_time, exception])

    def hook_locust_quit(self):
        self.request_failures_file.flush()
        self.request_failures_file.close()
