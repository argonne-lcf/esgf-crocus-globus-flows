from globus_compute_sdk import Client, Executor
import xarray as xr
from dotenv import load_dotenv
import os

# Extract variables from .env file
load_dotenv()
endpoint_uuid = os.getenv("ENDPOINT_UUID")
function_uuid = os.getenv("FUNCTION_UUID")
odir = os.getenv("ODIR")

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
    "odir": odir
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