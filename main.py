from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
from app.gql import gql_fetch_latest_stories, gql_fetch_media_statistics
import app.cronjob as cronjob
import app.config as config

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

@app.post('/cronjob/most_sponsor_publisher')
async def data_most_sponser_publisher():
  '''
  Generate top-[MOST_SPONSOR_PUBLISHERS_NUM] publishers which have most sponsors. 
  For each publisher, we also select the top-5 most recent stories. 
  '''
  MOST_SPONSOR_PUBLISHER_NUM = int(os.environ.get('MOST_SPONSOR_PUBLISHER_NUM', config.DEFAULT_MOST_SPONSOR_PUBLISHER_NUM))
  cronjob.most_sponsor_publisher(
    most_sponsors_num = MOST_SPONSOR_PUBLISHER_NUM
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
  cronjob.most_follower_members(
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

@app.post('/cronjob/media_statistics')
async def data_media_statistics():
  gql_endpoint = os.environ['MESH_GQL_ENDPOINT']
  media_statistics_days = int(os.environ.get('MEDIA_STATISTICS_DAYS', config.DEFAULT_MEDIA_STATISTICS_DAYS))
  all_stories = gql_fetch_media_statistics(gql_endpoint, media_statistics_days)
  cronjob.media_statistics(all_stories)
  return "ok"

@app.post('/cronjob/weekly_readr_posts')
async def data_weekly_readr_post():
  cronjob.recent_readr_stories(take=3)
  return "ok"

@app.post('/cronjob/hotpage_sponsored_publishers')
async def data_hotpage_sponsored_publishers():
  '''
    For main hotpage, we need to generate 3 most-sponsor publishers plus readr and their articles.
  '''
  cronjob.hotpage_most_sponsor_publisher()
  return "ok"

@app.post('/cronjob/hotpage_most_popular_story')
async def data_hotpage_most_popular_story():
  cronjob.hotpage_most_popular_story()
  return "ok"

@app.post('/cronjob/hotpage_most_like_comments')
async def data_hotpage_most_like_comments():
  cronjob.hotpage_most_like_comments()
  return "ok"

@app.post('/cronjob/open_publishers')
async def data_open_publishers():
  cronjob.open_publishers()
  return "ok"

@app.post('/cronjob/publisher_stories')
async def data_publisher_stories():
  cronjob.publisher_stories()
  return "ok"

@app.post('/cronjob/category_recommend_sponsors')
async def data_category_recommend_sponsors():
  cronjob.category_recommend_sponsors()
  return "ok"

@app.post('/cronjob/invalid_names')
async def data_invalid_names():
  cronjob.invalid_names()
  return "ok"

@app.post('/cronjob/check_transactions')
async def data_check_transactions():
  cronjob.check_transaction()
  return "ok"

@app.post('/cronjob/month_statements')
async def data_month_statements():
  cronjob.month_statements()
  return "ok"

@app.post('/cronjob/media_statements')
async def data_media_statements():
  cronjob.media_statements()
  return "ok"