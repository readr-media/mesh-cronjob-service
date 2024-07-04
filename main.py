from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.dispatcher import dispatch_feeder
import os

from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from redis import asyncio as aioredis
from app.gql import gql_fetch_latest_stories
from app.key_builder import key_builder
from app.cache import set_cache
import app.cronjob as cronjob
import app.config as config
from app.meilisearch_feed import index_setting

from datetime import datetime, timedelta
import pytz
import json

### App related variables
app = FastAPI()
origins = ["*"]
methods = ["*"]
headers = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins = origins,
    allow_credentials = True,
    allow_methods = methods,
    allow_headers = headers
)

### API Design
@app.get('/')
async def health_checking():
  '''
  Health checking API.
  '''
  return {"message": "Health check for mesh-feed-parser"}
  
@app.post('/feed')
async def feed_outer():
  '''
  This route is used to trigger the feeding process. It will dispatch
  feeders for each source, and the feeders will fetch the articles from
  their source and push them to the corresponding database.
  '''
  success_urls = dispatch_feeder()
  return {"message": f'export {len(success_urls)} stories onto db successfully'}

@app.post('/cronjob/most_sponsor_publisher')
async def data_most_sponser_publisher():
  '''
  Generate top-[MOST_SPONSOR_PUBLISHERS_NUM] publishers which have most sponsors. 
  For each publisher, we also select the top-[MOST_PICKCOUNT_PUBLISHER_NUM] pickCount stories. 
  '''
  MOST_SPONSOR_PUBLISHER_NUM = int(os.environ.get('MOST_SPONSOR_PUBLISHER_NUM', config.DEFAULT_MOST_SPONSOR_PUBLISHER_NUM))
  MOST_PICKCOUNT_PUBLISHER_NUM = int(os.environ.get('MOST_PICKCOUNT_PUBLISHER_NUM', config.MOST_PICKCOUNT_PUBLISHER_NUM))
  MOST_SPONSOR_STORY_DAYS = int(os.environ.get('MOST_SPONSOR_STORY_DAYS', config.DEFAULT_MOST_SPONSOR_STORY_DAYS))
  
  cronjob.most_sponsor_publisher(
    most_sponsor_publisher_num = MOST_SPONSOR_PUBLISHER_NUM,
    most_pickcount_publisher_num = MOST_PICKCOUNT_PUBLISHER_NUM,
    most_sponsor_story_days = MOST_SPONSOR_STORY_DAYS
  )
  return "ok"

@app.post('/cronjob/most_read_story')
async def data_most_pick_story():
  '''
  Cronjob to generate most_pick_stories based on different category
  '''
  gql_endpoint = os.environ['MESH_GQL_ENDPOINT']
  most_read_story_days = int(os.environ.get('MOST_READ_STORY_DAYS', config.DEFAULT_MOST_READ_STORY_DAYS))
  all_stories = gql_fetch_latest_stories(gql_endpoint, most_read_story_days)
  cronjob.most_read_story(all_stories)
  return "ok"

@app.post('/cronjob/most_followers')
async def data_most_followers():
  most_follower_num = int(os.environ.get('MOST_FOLLOWER_NUM', config.DEFAULT_MOST_FOLLOWER_NUM))
  cronjob.most_followers(
    most_follower_num=most_follower_num
  )
  return "ok"

@app.post('/cronjob/most_read_members')
async def data_most_read_members():
  most_read_member_num = int(os.environ.get('MOST_READ_MEMBER_NUM', config.DEFAULT_MOST_READ_MEMBER_NUM))
  most_read_member_days = int(os.environ.get('MOST_READ_MEMBER_DAYS', config.DEFAULT_MOST_READ_MEMBER_DAYS))
  cronjob.most_read_members(
    most_read_member_days=most_read_member_days, 
    most_read_member_num=most_read_member_num
  )
  return "ok"

@app.post('/category_latest')
async def category_latest():
  '''
  This route is used to load the latest articles of each category into redis.
  We will set the "days" parameter to control how many days we want to load.
  '''
  CATEGORY_LATEST_DAYS = int(os.environ.get('CATEGORY_LATEST_DAYS', config.DEFAULT_CATEGORY_LATEST_GQL_DAYS))
  gql_endpoint = os.environ['MESH_GQL_ENDPOINT']
  prefix = FastAPICache.get_prefix()
  
  current_time = datetime.now(pytz.timezone('Asia/Taipei'))
  all_stories = gql_fetch_latest_stories(gql_endpoint, CATEGORY_LATEST_DAYS)
  key_records = {}
  for story in all_stories:
    category_id = story.get('category', {}).get('id', None)
    source_id = story.get('source', {}).get('id', None)
    if category_id==None or source_id==None:
      continue
    key = key_builder(f"{prefix}:category_latest", f"{category_id}:{source_id}")
    key_stories = key_records.setdefault(key, [])
    key_stories.append(story)
  for key, value in key_records.items():
    expire_time = current_time + timedelta(seconds=config.DEFAULT_CATEGORY_LATEST_TTL)
    expire_timestamp = int(expire_time.timestamp())
    current_timestamp = int(current_time.timestamp())
    await set_cache(
      key, 
      json.dumps({"update_time": current_timestamp, "expire_time": expire_timestamp, "num_stories": len(value), "data": value}), 
      config.DEFAULT_CATEGORY_LATEST_TTL
    )
  print(f"successfully store {len(key_records.keys())} keys in the redis cache")
  return "ok"

@app.on_event("startup")
async def startup():
  ### set redis
  NAMESPACE = os.environ.get('NAMESPACE', 'dev')
  redis_endpoint = os.environ.get('REDIS_ENDPOINT', 'redis-cache:6379')
  redis = aioredis.from_url(f"redis://{redis_endpoint}", encoding="utf8", decode_responses=True)
  FastAPICache.init(RedisBackend(redis), prefix=f"{NAMESPACE}")