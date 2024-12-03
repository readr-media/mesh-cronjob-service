import pymongo

def connect_db(mongo_url: str, env: str='dev'):
    client = pymongo.MongoClient(mongo_url)
    db = None
    if env=='staging':
        db = client.staging
    elif env=='prod':
        db = client.prod
    else:
        db = client.dev
    return db