from globus_compute_sdk import Client, Executor
import xarray as xr

# Define UUIDs
endpoint_uuid = 'a93bab84-bc75-43a0-8ab9-ba7a41a1a2d4'
function_uuid = "093493c6-3786-4330-9ece-7efb5bb06116"

# Create Globus SDK Executor (currently using your own user credentials)
gcc = Client()
gce = Executor(endpoint_id=endpoint_uuid, client=gcc, amqp_port=443)

# Prepare payload for ESGF ingest-wxt
data = {
    "ndays": 1,
    "y": 2024,
    "m": 8,
    "d": 1,
    "site": 'NU',
    "hours": 1,
    "odir": "/home/rchard/src/CROCUS/output/"
}

# Start the task
future = gce.submit_to_registered_function(function_uuid, kwargs=data)

# Wait and print the result
result = future.result()
print(result)

# Preview one of the recently created file
if len(result) > 0:
    ds = xr.open_dataset(result[-1])
    print(ds)
