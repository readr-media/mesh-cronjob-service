import os
import app.feeder as feeder
from app.gql import gql_query, gql_mesh_publishers, gql_check_exist_stories
import threading

def feed_task(feeder: feeder.Feeder, export_stories: list, export_stories_lock):
  '''
    Use export_stoiries list to store stories and return back to main thread.
    You should be aware that export_stories is updated by multiple threads, should be thread safe
  '''
  stories = feeder.feed()
  export_stories_lock.acquire()
  try:
    export_stories.extend(stories)
  finally:
    export_stories_lock.release()

def dispatch_feeder():
    MESH_GQL_ENDPOINT = os.environ['MESH_GQL_ENDPOINT']
    export_stories = []
    export_stories_lock = threading.Lock()
    
    ### get publishers information
    publishers = gql_query(MESH_GQL_ENDPOINT, gql_mesh_publishers)
    publishers = publishers['publishers']
    
    ### assign feeder depends on source_type
    feeder_threads = []
    mm_external_medias = []
    for publisher in publishers:
        source_type = str(publisher.get('source_type', '')).lower()
        if source_type == 'rss':
            rss_feeder = feeder.RssFeeder(publisher)
            feeder_threads.append(threading.Thread(target=feed_task, args=(rss_feeder, export_stories, export_stories_lock)))
        if source_type == 'mm_external':
            mm_external_medias.append(publisher)
        if source_type == 'mirrormedia':
            mm_feeder = feeder.MirrorMediaFeeder(publisher)
            feeder_threads.append(threading.Thread(target=feed_task, args=(mm_feeder, export_stories, export_stories_lock)))
        if source_type == 'readr':
            readr_feeder = feeder.ReadrFeeder(publisher)
            feeder_threads.append(threading.Thread(target=feed_task, args=(readr_feeder, export_stories, export_stories_lock)))
    external_feeder = feeder.ExternalFeeder(mm_external_medias)
    feeder_threads.append(threading.Thread(target=feed_task, args=(external_feeder, export_stories, export_stories_lock)))
    
    ### start feeding
    for feeder_thread in feeder_threads:
        feeder_thread.start()

    ### wait for all the feeding threads finished
    for feeder_thread in feeder_threads:
        feeder_thread.join()
    
    ### filter out the stories which are already in db
    check_urls = [story['url'] for story in export_stories]
    gql_variable = {
        "where": {
            "url": {
                "in": check_urls
            }
        }
    }
    check_result = gql_query(MESH_GQL_ENDPOINT, gql_check_exist_stories, gql_variable)
    exist_urls = set([story['url'] for story in check_result['stories']])
    filtered_export_stories = [story for story in export_stories if story['url'] not in exist_urls]
    print(f"Nubmer of stories before filtering: {len(check_urls)}, after filtering: {len(filtered_export_stories)}")
    
    ### export stories to postgresql in single connection
    success_urls = []
    if len(export_stories)>0:
        export_feeder = feeder.RssFeeder(None)
        success_urls  = export_feeder.export_postgresql(filtered_export_stories)
    return success_urls