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

@app.post('/cronjob/media_statistics')
async def data_media_statistics():
  gql_endpoint = os.environ['MESH_GQL_ENDPOINT']
  media_statistics_days = int(os.environ.get('MEDIA_STATISTICS_DAYS', config.DEFAULT_MEDIA_STATISTICS_DAYS))
  all_stories = gql_fetch_media_statistics(gql_endpoint, media_statistics_days)
  cronjob.media_statistics(all_stories)
  return "ok"