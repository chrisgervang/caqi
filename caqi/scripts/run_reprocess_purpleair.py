from datetime import datetime
from prefect import Client
from prefect.core import parameter

client = Client()
client.create_flow_run(
    flow_id="0fadd5a7-4390-4a2b-b2cb-004a6fefc8cc", 
    parameters={
        'start': datetime(2020, 11, 23, 7).isoformat(), 
        'end_inclusive': datetime(2020, 11, 25, 8).isoformat(),
        'environment': 'prod'
    }
)
