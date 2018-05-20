# PostgresLocust
Postgres client for Locust.io

Uses the pg8000 Python Postgres client and SQLAlchemy for connection pooling.


Start the example postgre_locust_example.py

    python postgres_locust_example.py

Start the script with the web UI

    C:\Users\[user]\Anaconda3\Scripts\locust.exe -f postgres_locust_example.py

Start the script without the web UI

-c specifies the number of Locust users to spawn, and -r specifies the hatch rate (number of users to spawn per second).
--no-web to run without the UI
--csv=locust_results to automatically write results to csv files

    C:\Users\[user]\Anaconda3\Scripts\locust.exe -f postgres_locust_example.py --no-web --csv=locust_results