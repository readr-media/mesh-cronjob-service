import psycopg2
import os
from datetime import datetime, timedelta
import pytz
from app.tool import save_file, upload_blob
from app.gql import gql_query, gql_mesh_sponsor_publishers, gql_mesh_sponsor_stories, gql_mesh_publishers, gql_mesh_publishers_open, gql_recent_stories, gql_readr_id
import app.config as config

def most_followers(most_follower_num: int):
    data = []
    conn = psycopg2.connect(
      database = os.environ['DB_NAME'], 
      user = os.environ['DB_USER'], 
      password = os.environ['DB_PASS'], 
      host = os.environ['DB_HOST'], 
      port = os.environ['DB_PORT'],
    )
    conn.autocommit = True
    sql_member_follower = '''
      SELECT "A", count(*) FROM "_Member_follower" group by "A" 
      ORDER BY count DESC 
      LIMIT {TAKE};
    '''
    sql_member_follower_string = sql_member_follower.format(TAKE=most_follower_num)
    try:
        with conn.cursor() as cur:
            cur.execute(sql_member_follower_string)
            rows = cur.fetchall()
            if len(rows)==0:
              return False
            members = {row[0]: row[1] for row in rows}
            sql_member = '''
              SELECT id, name, nickname, avatar FROM "Member"
              WHERE id IN %s;
            '''
            cur.execute(sql_member, (tuple(members.keys()),))
            rows = cur.fetchall()
            for row in rows:
              id, name, nickname, avatar = row
              data.append({
                  "id": id,
                  "followerCount": members[id],
                  "name": name,
                  "nickname": nickname,
                  "avatar": avatar
              })
            data = sorted(data, key=lambda member: member['followerCount'], reverse=True)
    except Exception as error: 
      print("Error while get_most_followers:", error)
    finally:
      conn.close()
    filename = os.path.join('data', 'most_followers.json')
    save_file(filename, data)
    upload_blob(filename)
    return True
  
def most_read_members(most_read_member_days: int, most_read_member_num: int):
    current_time = datetime.now(pytz.timezone('Asia/Taipei'))
    start_time = current_time - timedelta(days=most_read_member_days)
    start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
      
    data = []
    conn = psycopg2.connect(
      database = os.environ['DB_NAME'], 
      user = os.environ['DB_USER'], 
      password = os.environ['DB_PASS'], 
      host = os.environ['DB_HOST'], 
      port = os.environ['DB_PORT'],
    )
    conn.autocommit = True
    sql_pick = '''
      SELECT member, count(*) FROM "Pick" 
      WHERE "Pick"."createdAt"> \'{START_TIME}\' AND "Pick".kind=\'read\' 
      GROUP BY member ORDER BY count DESC
      LIMIT {TAKE};
    '''
    sql_pick_string = sql_pick.format(START_TIME=start_time, TAKE=most_read_member_num)
    try:
        with conn.cursor() as cur:
            cur.execute(sql_pick_string)
            rows = cur.fetchall()
            if len(rows)==0:
              return False
            members = {row[0]: row[1] for row in rows}
            sql_member = '''
              SELECT id, name, nickname, email, avatar FROM "Member"
              WHERE id IN %s;
            '''
            cur.execute(sql_member, (tuple(members.keys()),))
            rows = cur.fetchall()
            for row in rows:
              id, name, nickname, email, avatar = row
              data.append({
                  "id": id,
                  "pickCount": members[id],
                  "name": name,
                  "nickname": nickname,
                  "email": email,
                  "avatar": avatar
              })
            data = sorted(data, key=lambda member: member['pickCount'], reverse=True)
    except Exception as error: 
      print("Error while get_most_followers:", error)
    finally:
      conn.close()
    ### upload data
    filename = os.path.join('data', 'most_read_members.json')
    save_file(filename, data)
    upload_blob(filename)
    return True
  
def most_read_story(all_stories: list):
    ### categorize stories
    categorized_stories = {}
    for story in all_stories:
      category_slug = story.get('category', {}).get('slug', None)
      if category_slug==None:
        continue
      story_list = categorized_stories.setdefault(category_slug, [])
      story_list.append(story)

    ### sorted by pick count for each category
    sorted_categorized_stories = {}
    for category_slug, story_list in categorized_stories.items():
      sorted_story_list = sorted(story_list, key=lambda story: story.get('picksCount', 0), reverse=True)
      sorted_categorized_stories[category_slug] = sorted_story_list[:config.DEFAULT_MOST_READ_STORY_NUM]
    
    ### save and upload json
    for category_slug, story_list in sorted_categorized_stories.items():
      filename = os.path.join('data', f'most_read_stories_{category_slug}.json')
      save_file(filename, story_list)
      upload_blob(filename)

def open_publishers():
  gql_endpoint = os.environ['MESH_GQL_ENDPOINT']
  all_publishers = gql_query(gql_endpoint, gql_mesh_publishers_open)
  all_publishers = all_publishers['publishers']
  publishers = {
    publisher['customId']: publisher for publisher in all_publishers
  }
  ### save and upload json
  filename = os.path.join('data', f'open_publishers.json')
  save_file(filename, publishers)
  upload_blob(filename)
  return True

def most_sponsor_publisher(most_sponsor_publisher_num: int, most_pickcount_publisher_num: int, most_sponsor_story_days: int):
  gql_endpoint = os.environ['MESH_GQL_ENDPOINT']
  current_time = datetime.now(pytz.timezone('Asia/Taipei'))
  start_time = current_time - timedelta(days=most_sponsor_story_days)
  formatted_start_time = start_time.isoformat()
  
  ### query data
  all_publishers = gql_query(gql_endpoint, gql_mesh_sponsor_publishers)
  all_publishers = all_publishers['publishers']
  publishers_mapping = {publisher['id']: publisher['title'] for publisher in all_publishers}
  
  ### TODO: Sort by SponsorCount(mock-data is sorted by followerCount)
  sorted_publishers = sorted(all_publishers, key=lambda publisher: publisher.get('sponsorCount', 0), reverse=True)
  sorted_publishers = sorted_publishers[:most_sponsor_publisher_num]
  
  ### Pick top-[MOST_PICKCOUNT_PUBLISHER_NUM] stories for each publisher
  sponsor_publisher_ids = [publisher['id'] for publisher in sorted_publishers]
  query_variable = {
    "where": {
      "source": {
        "id": {
          "in": list(sponsor_publisher_ids)
        },
      },
      "published_date":{
        "gte": str(formatted_start_time)
      }
    }
  }
  stories = gql_query(gql_endpoint, gql_mesh_sponsor_stories, query_variable)
  stories = stories['stories']
  
  sponsor_publisher_stories = {}
  for source_id in sponsor_publisher_ids:
    sponsor_publisher_stories[publishers_mapping[source_id]] = []
  for story in stories:
    source_id = story.get('source', {}).get('id', None)
    if source_id==None:
      continue
    sponsor_publisher_stories.setdefault(publishers_mapping[source_id], []).append(story)
  
  most_sponsor_publishers_stories = {}
  for source_id, story_list in sponsor_publisher_stories.items():
    sorted_story_list = sorted(story_list, key=lambda story: story.get('pickCount', 0), reverse=True)[:most_pickcount_publisher_num]
    most_sponsor_publishers_stories[source_id] = sorted_story_list
  
  ### Save and upload
  filename = os.path.join('data', f'most_sponsor_publishers.json')
  save_file(filename, sorted_publishers)
  upload_blob(filename)
  filename = os.path.join('data', f'most_sponsor_publishers_stories.json')
  save_file(filename, most_sponsor_publishers_stories)
  upload_blob(filename)
  
  return True

def media_statistics(all_stories: list):
  ### get all publishers and set default value
  statistics = {}
  gql_endpoint = os.environ['MESH_GQL_ENDPOINT']
  publishers = gql_query(gql_endpoint, gql_mesh_publishers)
  publishers = publishers['publishers']
  for publisher in publishers:
    id = publisher['id']
    title = publisher['title']
    statistics[id] = {
      "title": title,
      "readsCount": 0
    }
  
  ### categorize stories
  for story in all_stories:
    media_id = story['source']['id']
    readsCount = story['readsCount']
    statistics[media_id]['readsCount'] += readsCount
  
  ### save and upload json
  filename = os.path.join('data', f'media_statistics.json')
  save_file(filename, statistics)
  upload_blob(filename)
  
def recent_readr_stories(take: int):
  gql_endpoint = os.environ['MESH_GQL_ENDPOINT']
  
  # Get Readr stories
  # Note: You should avoid passing string comparison when sending stories query
  # Always use "id" to search stories 
  publishers = gql_query(gql_endpoint, gql_readr_id)
  publishers = publishers['publishers']
  readr_id = publishers[0]['id']
  stories = gql_query(gql_endpoint, gql_recent_stories.format(ID=readr_id, TAKE=take))
  stories = stories['stories']
  
  # Filter out the published_date
  current_time = datetime.now(pytz.timezone('Asia/Taipei'))
  start_time = current_time - timedelta(days=config.DEFAULT_RECENT_READR_DAYS)
  formatted_start_time = start_time.timestamp()
  filtered_stories = []
  for story in stories:
    published_date = story.get('published_date', None) # Example: "2024-08-30T03:00:00.000Z"
    if published_date==None:
      continue
    published_date_iso = datetime.strptime(published_date, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
    if published_date_iso>formatted_start_time:
      filtered_stories.append(story)
  
  ### save and upload json
  filename = os.path.join('data', f'recent_readr_stories.json')
  save_file(filename, filtered_stories)
  upload_blob(filename)
  