from datetime import datetime
from prefect import Client

client = Client()
client.create_flow_run(
    flow_id="b0e7cf52-8970-4662-b433-b6acd0395b49", 
    parameters={
        'start': datetime(2020, 11, 23, 7).isoformat(), 
        'end_inclusive': datetime(2020, 11, 26, 7).isoformat(),
        # 'environment': 'prod',
        'environment': 'staging'
    }
)
