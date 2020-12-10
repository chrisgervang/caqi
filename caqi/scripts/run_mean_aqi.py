from datetime import datetime
from prefect import Client

client = Client()
client.create_flow_run(
    flow_id="a4bab79c-055c-49b8-b7a7-cf66fa8bd6d2", 
    parameters={
        'start': datetime(2020, 11, 23, 7).isoformat(), 
        'end_inclusive': datetime(2020, 12, 8, 22).isoformat(),
        'environment': 'prod',
        # 'environment': 'staging'
    }
)
