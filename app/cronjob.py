import psycopg2
import os
from datetime import datetime, timedelta
import pytz
from app.tool import save_file, upload_blob, request_post
from app.gql import *
import app.config as config
import copy
from app.meilisearch import add_document
from app.mongo import connect_db
from app.tool import get_current_timestamp, gen_uuid

def most_follower_members(most_follower_num: int):
    MESH_GQL_ENDPOINT = os.environ['MESH_GQL_ENDPOINT']
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
            if len(rows)>0:
              members = {row[0]: row[1] for row in rows}
              sql_member = '''
                SELECT id, name, "customId", nickname, avatar, is_active FROM "Member"
                WHERE id IN %s;
              '''
              cur.execute(sql_member, (tuple(members.keys()),))
              rows = cur.fetchall()
              for row in rows:
                id, name, customId, nickname, avatar, is_active = row
                if is_active==False:
                  continue
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
      
    # If the legnth of data is less than most_follower_num, add new member info
    if len(data)<most_follower_num:
      ids = set([member['id'] for member in data])
      members = gql_query(MESH_GQL_ENDPOINT, gql_member_info.format(TAKE=most_follower_num))
      additional_members = members['members']
      for member in additional_members:
        id = int(member['id'])
        if id not in ids:
          member['id'] = id
          data.append(member)
        if len(data) >= most_follower_num:
          break
    
    # If the length of data is still empty, add dummy data
    if len(data)==0:
      data.append(config.DUMMY_MEMBER_INFO)
    
    filename = os.path.join('data', 'most_followers.json')
    save_file(filename, data)
    upload_blob(filename)
    return True
  
def most_read_members(most_read_member_days: int, most_read_member_num: int):
    MESH_GQL_ENDPOINT = os.environ['MESH_GQL_ENDPOINT']
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
            if len(rows)>0:
              members = {row[0]: row[1] for row in rows}
              sql_member = '''
                SELECT id, name, nickname, email, avatar, "customId", is_active FROM "Member"
                WHERE id IN %s;
              '''
              cur.execute(sql_member, (tuple(members.keys()),))
              rows = cur.fetchall()
              for row in rows:
                id, name, nickname, email, avatar, customId, is_active = row
                if is_active==False:
                  continue
                data.append({
                    "id": id,
                    "pickCount": members[id],
                    "name": name,
                    "nickname": nickname,
                    "email": email,
                    "avatar": avatar,
                    "customId": customId
                })
              data = sorted(data, key=lambda member: member['pickCount'], reverse=True)
    except Exception as error: 
      print("Error while get_most_followers:", error)
    finally:
      conn.close()
    
    # If the legnth of data is less than most_follower_num, add new member info
    if len(data)<most_read_member_num:
      ids = set([member['id'] for member in data])
      members = gql_query(MESH_GQL_ENDPOINT, gql_member_info.format(TAKE=most_read_member_num))
      additional_members = members['members']
      for member in additional_members:
        id = member['id']
        if id not in ids:
          data.append(member)
        if len(data) >= most_read_member_num:
          break
        
    # If the length of data is still empty, add dummy data
    if len(data)==0:
      data.append(config.DUMMY_MEMBER_INFO)
    
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
  
  ### save meilisearch
  try:
    search_publishers = []
    for publisher in all_publishers:
      search_publishers.append({
        "id": publisher['id'],
        "title": publisher['title'],
        "customId": publisher['customId'],
        "logo": publisher['logo'],
        "followerCount": publisher['followerCount'],
      })
    add_document(config.MEILISEARCH_PUBLISHER_INDEX, search_publishers)
  except Exception as e:
    print(f'Open publishers: add document failed, reason {e}')
  return True

def most_sponsor_publisher(most_sponsors_num: int):
  gql_endpoint = os.environ['MESH_GQL_ENDPOINT']
  
  ### query data
  all_publishers = gql_query(gql_endpoint, gql_mesh_sponsor_publishers)
  all_publishers = all_publishers['publishers']
  
  ### Sort by SponsorCount(mock-data is sorted by followerCount)
  sorted_publishers = sorted(all_publishers, key=lambda publisher: publisher.get('sponsorCount', 0), reverse=True)
  sorted_publishers = sorted_publishers[:most_sponsors_num]
  
  ### Pick top-[MOST_PICKCOUNT_PUBLISHER_NUM] stories for each publisher
  most_recommend_sponsors = []
  for publisher in sorted_publishers:
    id = publisher['id']
    query_variable = {
      "where": {
        "source": {
          "id": {
            "equals": id
          },
        }
      },
      "orderBy": {
        "id": "desc",
      },
      "take": 5,
    }
    stories = gql_query(gql_endpoint, gql_mesh_sponsor_stories, query_variable)
    stories = stories['stories'] if stories['stories'] else []
    most_recommend_sponsors.append({
      'publisher': publisher,
      'stories': stories,
    })
  
  ### Save and upload
  filename = os.path.join('data', f'most_recommend_sponsors.json')
  save_file(filename, most_recommend_sponsors)
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
  media_keys = set(statistics.keys())
  for story in all_stories:
    source = story['source']
    if source==None or isinstance(source, dict)==False:
      continue
    media_id = story['source']['id']
    if media_id not in media_keys:
      continue
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
  all_publishers = gql_query(gql_endpoint, gql_mesh_publishers)
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
  
def hotpage_most_popular_story(days: int=config.HOTPAGE_POPULAR_STORY_DAYS):
    ### get recent picks
    gql_endpoint = os.environ['MESH_GQL_ENDPOINT']
    current_time = datetime.now(pytz.timezone('Asia/Taipei'))
    start_time = current_time - timedelta(days=days)
    formatted_start_time = start_time.isoformat()
    
    # search for most popular story id
    mutation = {
    "where": {
            "published_date": {
                "gt": formatted_start_time
            }
        }
    }
    data = gql_query(gql_endpoint, gql_most_popular_story, mutation)
    stories = data['stories']
    most_popular_story = sorted(stories, key=lambda story: story['pickCount'], reverse=True)[0]

    # get full content
    story_id = most_popular_story['id']
    story = gql_query(gql_endpoint, gql_single_story.format(ID=story_id))
    
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
    
def publisher_stories():
    gql_endpoint = os.environ['MESH_GQL_ENDPOINT']
    publisher_stories = gql_fetch_publisher_stories(gql_endpoint, config.PUBLISHER_STORIES_NUM)
    if publisher_stories and isinstance(publisher_stories, dict):
      for filename, stories in publisher_stories.items():
        filename = os.path.join('data', filename)
        save_file(filename, stories)
        upload_blob(filename)
  
def category_recommend_sponsors():
    gql_endpoint = os.environ['MESH_GQL_ENDPOINT']
    proxy_endpoint = os.environ['MESH_PROXY_ENDPOINT']
    
    ### get publishers information
    publishers = gql_query(gql_endpoint, gql_mesh_publishers)
    publishers = publishers['publishers']
    publisher_table = {}
    statistic_template = {}
    for publisher in publishers:
        source_type = publisher['source_type']
        if source_type=='empty':
            continue
        id = publisher['id']
        publisher_table[id]= {
            'id': id,
            'title': publisher['title'],
            'logo': publisher['logo'],
            'customId': publisher['customId'],
            'official_site': publisher['official_site'],
            'sponsoredCount': publisher['sponsoredCount'],
        }
        statistic_template[id] = 0
    all_publisher_ids = list(publisher_table.keys())
    
    ### get category information
    categories = gql_query(gql_endpoint, gql_mesh_categories)
    categories = categories['categories']
    category_table = {}
    for category in categories:
        id = category['id']
        slug = category['slug']
        if 'test' not in slug: 
            category_table[id]= slug
            
    ### recommend sponsored publishers, we get the data from redis
    recommend_sponsor_table = {}
    for category_id, category_slug in category_table.items():
        # get data for this category
        body = {
            "publishers": all_publisher_ids,
            "category": category_id
        }
        stories, error_msg = request_post(proxy_endpoint, body)
        stories = stories['stories']
        if error_msg:
            print(f"something wrong when processing category {category_id}, error: {error_msg}")
            continue

        # calculate statistic
        statistic_table = {} # count publisher_id and readsTotal mapping
        publisher_stories = {} # keep publisher_id and stories mapping
        for story in stories:
            publisher_id = story['source']['id']
            readsCount = story['picksCount']
            statistic_table[publisher_id] = statistic_table.get(publisher_id, 0) + readsCount
            story_list = publisher_stories.setdefault(publisher_id, [])
            story_list.append({
                "id": story['id'],
                "url": story['url'],
                "title": story['title'],
                "published_date": story['published_date'],
                "og_title": story["og_title"],
                "og_image": story["og_image"],
                "og_description": story["og_description"],
                "full_screen_ad": story["full_screen_ad"],
                "full_content": story["full_content"],
                "commentCount": story['commentCount'],
                "readsCount": readsCount
            })

        # sorting
        recommend_publishers = sorted(statistic_table.items(), key=lambda item: item[1], reverse=True)[:config.RECOMMEND_SPONSOR_PUBLISHER_NUM]
        recommend_publishers = [publisher[0] for publisher in recommend_publishers]
        for publisher_id in recommend_publishers:
            stories = publisher_stories.get(publisher_id, [])
            sorted_stories = sorted(stories, key=lambda item: item['readsCount'], reverse=True)[:config.RECOMMEND_SPONSOR_STORY_NUM]
            
            sponsor_list = recommend_sponsor_table.setdefault(category_slug, [])
            sponsor_list.append({
                "publisher": publisher_table[publisher_id],
                "stories": sorted_stories
            })
    
    ### save and upload json
    for category_slug, publisher_stories in recommend_sponsor_table.items():
        filename = os.path.join('data', f'{category_slug}_recommend_sponsors.json')
        save_file(filename, publisher_stories)
        upload_blob(filename)

def invalid_names():
    gql_endpoint = os.environ['MESH_GQL_ENDPOINT']
    names = gql_query(gql_endpoint, gql_invalid_names)
    names = names['invalidNames']
    
    # convert names to lowercase
    names = [name['name'].lower() for name in names]
    
    # save and upload json
    filename = os.path.join('data', f'invalid_names.json')
    save_file(filename, names)
    upload_blob(filename)
    
def check_transaction():
    mongo_url = os.environ['MONGO_URL']
    env = os.environ.get('ENV', 'dev')
    gql_endpoint = os.environ['MESH_GQL_ENDPOINT']
    db = connect_db(mongo_url, env)
    
    # Fetch transactions
    current_time = datetime.datetime.now(pytz.timezone('Asia/Taipei'))
    end_time = current_time + datetime.timedelta(days=config.TRANSACTION_NOTIFY_DAYS)
    expire_date = end_time.isoformat()
    
    data = gql_query(gql_endpoint, gql_expire_transactions.format(EXPIRE_DATE = expire_date))
    transactions = data['transactions']
    if len(transactions)==0:
        return True
    
    # Categorize transactions
    cur_timestamp = get_current_timestamp()
    expired_txs, approach_expire_txs = [], []
    for tx in transactions:
        expire_time = tx['expireDate']
        expire_timestamp = datetime.datetime.strptime(expire_time, '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()
        if expire_timestamp > cur_timestamp:
            approach_expire_txs.append(tx)
        else:
            expired_txs.append(tx)
    
    # Update expired txs active to False
    try:
        var = {
            "data": [
                {
                    "where": {
                        "id": tx['id']
                    },
                    "data": {
                        "active": False
                    }
                }
                for tx in expired_txs
            ] 
        }
        data = gql_query(gql_endpoint, gql_disable_transactions, var)
        disable_txs = data['updateTransactions']
        print("Disable expired transactions number: ", len(disable_txs))
    except Exception as e:
        print("Failed to update transactions state, reason: ", str(e))
    
    ### notify approach expire txs
    # categorize approach expire txs into different memberId
    categorized_expire_txs = {}
    for tx in approach_expire_txs:
        try:
            transactionId = tx['id'] 
            memberId = tx['member']['id']
            notify_tx = {
                "uuid": gen_uuid(),
                "read": False,
                "action": "approach_expiration",
                "objective": "transaction",
                "targetId": transactionId,
                "ts": cur_timestamp,
                "content": {
                    "id": transactionId,
                    "status": tx["status"],
                    "expireDate": tx["expireDate"],
                    "policy": tx["policy"],
                    "unlockStory": tx["unlockStory"]
                }
            }
            notify_list = categorized_expire_txs.setdefault(memberId, [])
            notify_list.append(notify_tx)
        except Exception as e:
            print("Fail to notify transactionId: ", tx)

    ### notify members
    col_notify = db.notifications
    for memberId, new_notifies in categorized_expire_txs.items():
        record = col_notify.find_one(memberId)
        if record==None:
            record = {
                "_id": memberId,
                "lrt": 0,
                "notifies": new_notifies
            }
            col_notify.insert_one(record)
        else:
            all_notifies = record['notifies']

            # organize old approach expiration notifies
            published_expiration_notifies = set()
            for notify in all_notifies:
                action = notify['action']
                objective = notify['objective']
                targetId = notify['targetId']
                if action=='approach_expiration' and objective=='transaction':
                    published_expiration_notifies.add(targetId)
            
            # check if the notification already exist
            for notify in new_notifies:
                targetId = notify['targetId']
                if targetId not in published_expiration_notifies:
                    all_notifies.insert(0, notify)
                    print("Insert new notify", notify)
            all_notifies = all_notifies[:config.MOST_NOTIFY_RECORDS]
            col_notify.update_one(
                {"_id": memberId},
                {"$set": {"notifies": all_notifies}}
            )
    return True