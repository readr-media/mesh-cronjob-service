import os
import meilisearch

def add_document(index, data):
    '''
      Store data into Meilisearch index. data should be a list with dict content.
    '''
    meilisearch_host = os.environ['MEILISEARCH_HOST']
    meilisearch_apikey = os.environ['MEILISEARCH_APIKEY']
    client = meilisearch.Client( meilisearch_host, meilisearch_apikey)
    try:
        response = client.index(index).add_documents(data, primary_key="id")
        print(response)
    except Exception as e:
        print(f'add document failed, reason: {e}')