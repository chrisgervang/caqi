from datetime import datetime
from prefect import Client

client = Client()
client.create_flow_run(
    flow_id="a79fe5cb-b8e0-4824-a750-1934a68045c4", 
    parameters={
        'dt': datetime(2020, 11, 25, 12).isoformat(), 
        # 'environment': 'prod',
        'environment': 'staging'
    }
)
