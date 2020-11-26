from datetime import timedelta
from prefect import task

@task
def datetime_range(start, interval_hour, end):
    current_time = start
    datetimes = []
    while current_time <= end:
        datetimes.append(current_time)
        current_time += timedelta(hours=interval_hour)
    return datetimes