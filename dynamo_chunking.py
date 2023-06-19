from decimal import Decimal
from botocore.exceptions import ClientError
import concurrent.futures
import pandas as pd
import time, boto3, sys, json

def process_chunk(chunk, table, offset, increment):
    with table.batch_writer() as batch:
        for index, data in chunk.iterrows():
            row_count = index
            while True:
                try:
                    # print("Rendering data...")
                    batch.put_item(json.loads(data.to_json(), parse_float=Decimal))
                    # print(f'saved{str(row_count)}')
                except ClientError:
                    time.sleep(5)
                    continue
                break
    print(50 * "*")
    offset += increment
    print(f'-- batch finished: {offset:,} --')

def process_dynamodb(chunk_size, batch: int):
    ACCESS_KEY = "{}" # AWS Access Key
    SECRET_KEY = "{}" # AWS Secret Key
    TABLE_NAME = "{}" # DynamoDB Table Name
    REGION_NAME = "{}", # AWS Region

    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION_NAME
    )
    table = dynamodb.Table(TABLE_NAME)
    increment = 5000
    batch_iteration = 59

    starting_number = 160800000
    #offset = 137790000  # naka-assign na number sa notepad

    offset = starting_number + ((batch - 1) * batch_iteration * increment)
    next_batch_offset = starting_number + ((batch) * batch_iteration * increment)
    print(f"Running script for batch #{batch} of offset #{offset:,} to {next_batch_offset:,}")

    try:
        st = time.perf_counter()
        for chunk_num, chunk_df in enumerate(pd.read_csv('poc_data_5K.csv', chunksize=chunk_size)):
            new_df = chunk_df.copy()
            new_df['column1'] = new_df['column1'] + offset

            process_chunk(new_df, table, offset, increment)

            offset += (increment * chunk_size)
            print(f'-- Chunk {chunk_num + 1} finished in {time.perf_counter() - st} seconds --')

        print(f'-- dynamodb loading finished all in {time.perf_counter() - st} seconds! --')
    except KeyboardInterrupt:
        print("You pressed Ctrl + C.")


if __name__ == "__main__":
    chunk_size = int(sys.argv[1]) # for example - python3 .\dynamo_testing.py 100
    #print(f"chunck size: {chunk_size}")  # define the size of each chunk you want to process
    batch_count = int(sys.argv[2]) # up to 18 batch scripts
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        
        for batch in range(1, batch_count+1):
            futures.append(executor.submit(process_dynamodb, chunk_size=chunk_size, batch=batch))
        
        for future in concurrent.futures.as_completed(futures):
            print(future.result())

    # run this in CLI: python3 .\dynamo_testing.py 100
