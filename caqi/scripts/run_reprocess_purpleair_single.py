from datetime import datetime
from prefect import Client

client = Client()
client.create_flow_run(
    flow_id="dc691fe0-61da-4394-ac1e-4f2cce561935", 
    parameters={
        'dt': datetime(2020, 11, 25, 12).isoformat(), 
        # 'environment': 'prod',
        'environment': 'staging'
    }
)
