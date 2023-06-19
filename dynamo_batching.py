import json
import pandas as pd
import boto3
import time
from decimal import Decimal
from botocore.exceptions import ClientError
import sys
import datetime


def lapse_time(sec):
    return f"{datetime.timedelta(seconds=sec)}"


start = time.time()

## python dynamo_ingest_batching.py <batch number>
## Eg. `python dynamo_ingest_batching.py 1` will run batch 1

print(sys.argv)
batch = int(sys.argv[1])  # BATCH NUMBER Eg dynamo_ingest_01.py is Batch 1
starting_number = 175277000  # naka-assign na number sa notepad

# UP TO 18 batch scripts
# 1062 / 18 = 59 batch iteration
# 59 * 364(estimated secs per iteration) = 21476 secs
# 21476 = 05:57:56 (estimated time)
batch_iteration = 59

ACCESS_KEY = "{}" # AWS Access Key
SECRET_KEY = "{}" # AWS Secret Key
table_name = "{}" # DynamoDB Table Name
df = pd.read_csv("{}") # Base CSV to load

# dynamodb = boto3.resource('dynamodb', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

# Region error resolve
dynamodb = boto3.resource(
    "dynamodb",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name="{}", # AWS Region
)

table = dynamodb.Table(table_name)
row_count = 0
increment = 5000

offset = starting_number + ((batch - 1) * batch_iteration * increment)
next_batch_offset = starting_number + ((batch) * batch_iteration * increment)
print(f"Running script for batch #{batch} of offset #{offset} to {next_batch_offset}")

for i in range(1, batch_iteration + 1):
    new_df = df.copy()

    new_df["column1"] = new_df["column1"] + offset
    with table.batch_writer() as batch:
        for index, data in new_df.iterrows():
            row_count = index
            while True:
                try:
                    batch.put_item(json.loads(data.to_json(), parse_float=Decimal))

                except ClientError:
                    print(
                        "provision exceeded. pausing for 5 seconds.. row: "
                        + str(row_count)
                    )
                    time.sleep(5)
                    continue
                break

    offset += increment
    print(f"batch finished:{offset} \tIteration #{i} out of {batch_iteration}")

    #################################
    secs = int(time.time() - start)
    print(f"{lapse_time(secs)} lapsed.")
    speed = secs // (i)
    estimated_remaining_time = ((batch_iteration) - (i)) * speed
    print(
        f"Estimated remaining time {lapse_time(estimated_remaining_time)}. Speed : {speed} secs/iteration"
    )
    #################################

print(f"dynamodb loading finished for batch #{batch} of assigned #{starting_number}!")
