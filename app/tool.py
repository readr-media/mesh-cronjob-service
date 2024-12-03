import os
import json
from google.cloud import storage
import app.config as config
import requests
import uuid
import datetime

### upload
def upload_blob(dest_filename, cache_control: str = 'cache_control_short'):
    ### with service account attached to the service
    storage_client = storage.Client()
    bucket = storage_client.bucket(os.environ['BUCKET'])
    blob = bucket.blob(dest_filename)
    blob.upload_from_filename(dest_filename)
    blob.cache_control = config.upload_configs[cache_control]
    blob.patch()
    print(f'upload {dest_filename} to blob successfully')
    
### files operations
def save_file(dest_filename, data):
    if data:
        dirname = os.path.dirname(dest_filename)
        if len(dirname)>0 and not os.path.exists(dirname):
            os.makedirs(dirname)
        with open(dest_filename, 'w', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False))
        print(f'save {dest_filename} successfully')

def open_file(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        file = json.load(f)
    return file

def request_post(endpoint: str, body: dict):
    json_data, error_message = None, None
    try:
        response = requests.post(endpoint, json=body, timeout=config.DEFAULT_REQUEST_TIMEOUT)
        json_data = response.json()
    except Exception as e:
        error_message = e
    return json_data, error_message

def gen_uuid():
    return str(uuid.uuid4())[:8]

def get_current_timestamp():
    return int(datetime.datetime.now().timestamp())