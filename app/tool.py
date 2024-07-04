import os
import json
from google.cloud import storage
from app.config import upload_configs

### upload
def upload_blob(dest_filename, cache_control: str = 'cache_control_short'):
    ### with service account attached to the service
    storage_client = storage.Client()
    bucket = storage_client.bucket(os.environ['BUCKET'])
    blob = bucket.blob(dest_filename)
    blob.upload_from_filename(dest_filename)
    blob.cache_control = upload_configs[cache_control]
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