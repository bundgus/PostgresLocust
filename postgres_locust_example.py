from locust import TaskSet, task
from postgreslocust import PostgresLocust
from locust.main import main
import sys
import os
import json


# noinspection SqlResolve,SqlNoDataSourceInspection
class PostgresLocustClientTasks(TaskSet):
    @task(1)
    def query1(self):
        query1 = '''
        select count(*) 
        from table1
        '''
        self.client.query1(query1)  # this is where the results get the label 'query1'  ...client.[querylabel](...

    @task(2)
    def query2(self):
        query2 = '''
        select * from table1
        '''
        self.client.query2(query2)  # this is where the results get the label 'query2'  ...client.[querylabel](...


class PostgresLocustClient(PostgresLocust):

    with open('database_login_params.json') as db_params_json_file:
        db_params = json.load(db_params_json_file)

    #need host, port, dbname, user, and password
    host = db_params['host']
    port = int(db_params['port'])  # redshift is 5439
    dbname = db_params['dbname']
    user = db_params['user']
    password = db_params['password']

    pool_size = 1  # maximum number of concurrent queries per locust before they block

    min_wait = 500
    max_wait = 1000

    task_set = PostgresLocustClientTasks


if __name__ == "__main__":
    # http://localhost:8089/
    locusts = 1  # specifies the number of Locust users to spawn
    hatch_rate = 1  # -r specifies the hatch rate (number of users to spawn per second)

    # starting with locust 0.9
    # -t Stop after the specified amount of time, e.g. (300s, 20m, 3h, 1h30m, etc.).

    args = ['-f', os.path.basename(__file__),
            '--no-web', '--csv=locust_results',
            '-c', str(locusts),
            '-r', str(hatch_rate)]
    old_sys_argv = sys.argv
    sys.argv = [old_sys_argv[0]] + args
    main()
