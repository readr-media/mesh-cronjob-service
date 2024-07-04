import meilisearch
import os

def add_document(index, stories):
    meilisearch_host = os.environ['MEILISEARCH_HOST']
    meilisearch_apikey = os.environ['MEILISEARCH_APIKEY']
    if meilisearch_apikey:
        client = meilisearch.Client( meilisearch_host, meilisearch_apikey)
    else:
        client = meilisearch.Client( meilisearch_host)

    try:
        result = client.index(index).add_documents(stories)
        print(result)
    except:
        if len(stories) == 1:
            print(stories)

def index_setting(index):
    meilisearch_host = os.environ['MEILISEARCH_HOST']
    meilisearch_apikey = os.environ['MEILISEARCH_APIKEY']
    if meilisearch_apikey:
        client = meilisearch.Client( meilisearch_host, meilisearch_apikey)
    else:
        client = meilisearch.Client( meilisearch_host)

    print(client.index(index).get_settings())
    try:
        result = client.index(index).update_settings({
          'searchableAttributes': [
              'title',
              'summary',
              'content',
              'source'
          ],
        })
        print(result)
    except:
        print("searchableAttributes setting failed")
    try:
        result = client.index(index).update_filterable_attributes({
          [ 'type' ],
        })
    except:
        print("filterableAttributes setting failed")
    try:
        client.index(index).update_settings({
              'rankingRules': [
                  'words',
                  'typo',
                  'proximity',
                  'attribute',
                  'sort',
                  'exactness',
                  'release_date:asc',
                  'rank:desc'
              ]
            })
    except:
        print("ranking rules setting failed")