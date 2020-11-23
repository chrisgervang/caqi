from prefect import Client

client = Client()
client.create_flow_run(flow_id="c21abe1e-15c4-46f8-b04c-6a1a89e82a09")
