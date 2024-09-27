import psycopg2
import os
from datetime import datetime, timedelta
import pytz
from app.tool import save_file, upload_blob
from app.gql import *
import app.config as config
import copy

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
              SELECT id, name, "customId", nickname, avatar FROM "Member"
              WHERE id IN %s;
            '''
            cur.execute(sql_member, (tuple(members.keys()),))
            rows = cur.fetchall()
            for row in rows:
              id, name, customId, nickname, avatar = row
              data.append({
                  "id": id,
                  "followerCount": members[id],
                  "name": name,
                  "nickname": nickname,
                  "customId": customId,
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
    gql_endpoint = os.environ['MESH_GQL_ENDPOINT']
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
    
    ### get the comment with most likes for the first story of each category
    for category_slug, story_list in sorted_categorized_stories.items():
      story_id = story_list[0]['id']
      most_like_comment = get_most_like_comment(gql_endpoint, story_id)
      sorted_categorized_stories[category_slug][0]['comment'] = most_like_comment
    
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
  publishers = gql_query(gql_endpoint, gql_readr_info)
  readr_info = publishers['publishers'][0]
  readr_id = readr_info['id']
  stories = gql_query(gql_endpoint, gql_recent_stories_pick.format(ID=readr_id, TAKE=take))
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
  # If no stories recently, append the first story
  if len(filtered_stories)==0:
    filtered_stories.append(stories[0])
  readr_info['stories'] = filtered_stories
  
  ### save and upload json
  filename = os.path.join('data', f'recent_readr_stories.json')
  save_file(filename, readr_info)
  upload_blob(filename)

def hotpage_most_sponsor_publisher():
  gql_endpoint = os.environ['MESH_GQL_ENDPOINT']
  all_publishers = gql_query(gql_endpoint, gql_mesh_publishers_sponsor)
  all_publishers = all_publishers['publishers']
  
  ### filter readr
  sponsor_readr = {}
  for publisher in all_publishers:
      if publisher.get('title', '').lower()=='readr':
          sponsor_readr = copy.deepcopy(publisher)
          all_publishers.remove(publisher)
          break
        
  ### find the top-N sponsored count, append readr in the end
  most_sponsored_publishers = sorted(all_publishers, key=lambda publisher: publisher.get('sponsoredCount', 0), reverse=True)[:config.HOTPAGE_SPONSOR_PUBLISHER_NUM]
  most_sponsored_publishers.append(sponsor_readr)

  ### fetch top-N most recent stories for each publisher
  for idx, publisher in enumerate(most_sponsored_publishers):
      id = publisher['id']
      gql_stories = gql_recent_stories_comment.format(ID=id, KIND='read', TAKE=config.HOTPAGE_SPONSOR_PUBLISHER_STORY_NUM)
      all_stories = gql_query(gql_endpoint, gql_stories)['stories']
      most_sponsored_publishers[idx]['stories'] = all_stories
  
  ### save and upload json
  filename = os.path.join('data', f'hotpage_most_sponsored_publisher.json')
  save_file(filename, most_sponsored_publishers)
  upload_blob(filename)
  
def hotpage_most_popular_story():
    ### get recent picks
    gql_endpoint = os.environ['MESH_GQL_ENDPOINT']
    gql_recent_reads_str = gql_recent_reads.format(TAKE=config.HOTPAGE_POPULAR_STORY_READS_NUM)
    recent_reads = gql_query(gql_endpoint, gql_recent_reads_str)
    recent_reads = recent_reads['picks']

    ### calculate each story's counts
    read_statistics = {}
    for read in recent_reads:
        story = read['story']
        if story==None or isinstance(story, dict)==False:
            continue
        id = story.get('id', None)
        if id==None:
            continue
        read_statistics[id] = read_statistics.get(id, 0) + 1

    ### sort and take the most popular one
    most_reads_story = sorted(read_statistics.items(), key=lambda item: item[1], reverse=True)[0]
    story = gql_query(gql_endpoint, gql_single_story.format(ID=most_reads_story[0]))
    
    ### save and upload json
    filename = os.path.join('data', f'hotpage_most_popular_story.json')
    save_file(filename, story)
    upload_blob(filename)
    
def hotpage_most_like_comments():
    ### get recent comments
    gql_endpoint = os.environ['MESH_GQL_ENDPOINT']
    comments = gql_query(gql_endpoint, gql_comment_statistic.format(TAKE=config.HOTPAGE_RECENT_COMMENTS_NUM))
    comments = comments['comments']

    ### sort comment by likeCount
    sorted_comments = sorted(comments, key=lambda item: item['likeCount'], reverse=True)[:config.HOTPAGE_MOST_LIKE_COMMENTS_NUM]
    search_ids = [comment['id'] for comment in sorted_comments]
    search_ids

    ### get the detail of each comment
    variable = {
        "where": {
            "id": {
                "in": search_ids
            }
        }
    }
    most_like_comments = gql_query(gql_endpoint, gql_comment_detail, variable)
    most_like_comments = most_like_comments['comments']
    sorted_most_like_comments = sorted(most_like_comments, key=lambda comment: comment.get('likeCount', 0), reverse=True)
    
    ### save and upload json
    filename = os.path.join('data', f'hotpage_most_like_comments.json')
    save_file(filename, sorted_most_like_comments)
    upload_blob(filename)