import os
import datetime
import boto3
import time

from multiprocessing import Process
from multiprocessing import Queue
from queue import Empty
from tqdm import tqdm

ACCESS_KEY = "{}" # AWS Access Key
SECRET_KEY = "{}" # AWS Secret Key
REGION_NAME = "{}", # AWS Region
BUCKET =  "{}", # AWS S3 Bucket
KEY = "{}" # AWS S3 Bucket Key
PART_BYTES = int(700e6) #700 MB
parts_to_upload = []

### non object plain methods

def abort_all(s3):
    mpus = s3.list_multipart_uploads(Bucket=BUCKET)
    aborted = []
    print("Aborting", len(mpus), "uploads")
    if "Uploads" in mpus:
      for u in mpus["Uploads"]:
        upload_id = u["UploadId"]
        aborted.append(
            s3.abort_multipart_upload(
                Bucket=BUCKET, Key=KEY, UploadId=upload_id))
    return aborted

def complete(s3, mpu_id, parts):
  result = s3.complete_multipart_upload(
     Bucket=BUCKET,
      Key=KEY,
      UploadId=mpu_id,
      MultipartUpload={"Parts": parts})
  return result

def abort_all(s3):
  mpus = s3.list_multipart_uploads(Bucket=BUCKET)
  aborted = []
  print("Aborting", len(mpus), "uploads")
  if "Uploads" in mpus:
    for u in mpus["Uploads"]:
      upload_id = u["UploadId"]
      aborted.append(s3.abort_multipart_upload(Bucket=BUCKET, Key=KEY, UploadId=upload_id))
  return aborted

def create(s3):
  mpu = s3.create_multipart_upload(Bucket=BUCKET, Key=KEY)
  mpu_id = mpu["UploadId"]
  return mpu_id

def complete(s3, mpu_id, parts):
  result = s3.complete_multipart_upload(
        Bucket=BUCKET,
        Key=KEY,
        UploadId=mpu_id,
        MultipartUpload={"Parts": parts})
  return result

def get_part_index(filename, index=0):
    ctr = index
    #total_bytes = os.stat(filename).st_size
    with open(filename, "rb") as f:
      while True:
        data = f.read(PART_BYTES)
        if not len(data):
          break
        ctr += 1
    return ctr


# generate work
def producer(queue):
    print('Producer: Running', flush=True)

    fname_format = "UC4-Y3-5K-{}.csv"

    #generate work
    #part_index = 1
    for index in range(1, 801):
       file_name = fname_format.format(index)
       # put work in queue here
       print(f'sending file: {file_name} for work. starting part number: {index}')
       queue.put(f'{file_name}@{index}')
       #part_index = get_part_index(file_name, part_index)

    # all done put None work for workers to stop waiting. 8 workers
    queue.put(None)
    queue.put(None)
    queue.put(None)
    queue.put(None)
    queue.put(None)
    queue.put(None)
    queue.put(None)
    queue.put(None)

    print('Producer: Done', flush=True)

# consume work
def consumer(name, s3, mpu_id, queue):
    print(f'worker {name} starting to work', flush=True)
    # consume work
    while True:
        try:
            # get a unit of work
            item = queue.get()
        
            # check for stop
            if item is None:
                print(f'worker {name} finishing work..', flush=True)
                break
            # report
            print(f'worker {name} starting to work on {item}', flush=True)
            #s3, mpu_id, filename, index
            upload(s3, mpu_id, item.split('@')[0], int(item.split('@')[1]))
            #parts_to_upload.extend(p)
            # invoke work here

        except Empty:
            print(f'worker {name} got empty on work', flush=True)
            pass

    # all done
    print(f'worker {name}: Done', flush=True)
    #return None


def get_parts():
    parts = []
    with open("parts.txt") as f:
        for line in f:
            d = {}
            (number, tag) = line.split(',')
            d["PartNumber"] = int(number.strip())
            d["ETag"] = tag.strip()
            parts.append(d)

    return parts


def save_part(part_number: int, e_tag):
    db_file = open('parts.txt', 'a')
    db_file.write(f'{part_number},{e_tag}\n')
    db_file.close()

def upload(s3, mpu_id, filename, index):
    #upload_parts = []
    #uploaded_bytes = 0

    with open(filename, "rb") as f:
      #i = index
      #pbar = tqdm(total=total_bytes)
      #while True:
      data = f.read()
      #  if not len(data):
      #    break
      part = s3.upload_part(Body=data, 
                            Bucket=BUCKET, 
                            Key=KEY, 
                            UploadId=mpu_id, 
                            PartNumber=index)        
        
      save_part(part_number=index, e_tag=part["ETag"])
        #parts_to_upload.append({"PartNumber": i, "ETag": part["ETag"]})
        #upload_parts.append({"PartNumber": i, "ETag": part["ETag"]})
        #uploaded_bytes += len(data)
        #pbar.update(len(data))
        #i += 1

      #pbar.close()
    
    #return upload_parts

def lapse_time(sec):
  return f"{datetime.timedelta(seconds=sec)}"




def main():
    
    if os.path.exists("parts.txt"): os.remove("parts.txt")

    s3 = boto3.session.Session(aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY,
        profile_name=None, region_name=REGION_NAME).client("s3")

    print('starting multi-part upload to s3..')
    queue = Queue()
    start_time = time.time()
    mpu_id = create(s3)
    print(f'{mpu_id} upload created..')


    consumer1 = Process(target=consumer, args=('1', s3, mpu_id, queue,))
    consumer1.start()

    consumer2 = Process(target=consumer, args=('2',s3, mpu_id, queue,))
    consumer2.start()

    consumer3 = Process(target=consumer, args=('3',s3, mpu_id, queue,))
    consumer3.start()

    consumer4 = Process(target=consumer, args=('4',s3, mpu_id, queue,))
    consumer4.start()

    consumer5 = Process(target=consumer, args=('5',s3, mpu_id, queue,))
    consumer5.start()

    consumer6 = Process(target=consumer, args=('6',s3, mpu_id, queue,))
    consumer6.start()

    consumer7 = Process(target=consumer, args=('7',s3, mpu_id, queue,))
    consumer7.start()

    consumer8 = Process(target=consumer, args=('8',s3, mpu_id, queue,))
    consumer8.start()

    producer1 = Process(target=producer, args=(queue,))
    producer1.start()

    # wait for all processes to finish
    producer1.join()
    consumer1.join()
    consumer2.join()
    consumer3.join()
    consumer4.join()
    consumer5.join()
    consumer6.join()
    consumer7.join()
    consumer8.join()

    #after work finish, complete the upload
    parts = get_parts()
    p = sorted(parts, key=lambda d: d['PartNumber']) 
    print(p)
    print(f'{mpu_id} completing upload..')
    complete(s3, mpu_id, p)
    secs = int(time.time() - start_time)
    print(f'POC-Y3.csv merged. process time: {lapse_time(secs)}')
  

if __name__ == "__main__":
    main()