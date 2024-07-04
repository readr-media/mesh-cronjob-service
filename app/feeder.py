import os
import feedparser
import requests
import regex as re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from abc import ABC, abstractmethod
from bs4 import BeautifulSoup as bs
from app.gql import gql_query, gql_readr_posts, gql_mesh_create_stories, gql_mm_partners, gql_mm_externals, gql_mm_posts
import app.config as config 
from gql.transport.requests import RequestsHTTPTransport
from gql.transport.exceptions import TransportError
from gql import gql, Client
import json
import app.feeder as feeder

### used in postgresql client
import psycopg2
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter

### Tools you'll use when you feed data
def parse_timestring(timestring, publisher_name):
    '''
    parse the timestring into same format for different publisher, the format of publishers listed below
    (1) timeformat = '2022-06-14T20:31:07+08:00'
    (2) ETToday: Wed,15 Jun 2022 10:33:00  +0800
    (3) CNA: Tue, 14 Jun 2022 18:46:39 +0800
    (4) rightplus: Thu, 21 Apr 2022 05:36:18 +0000
    (5) twreporter: Sun, 21 Apr 2024 16:00:00 GMT
    '''
    ### parse the timestring into same format for different publisher
    datetime_string = timestring
    publisher_name = publisher_name.lower()
    if timestring==None:
        return datetime_string
    # publisher below should be parsed specially
    if publisher_name == 'twreporter':
        pubDate = datetime.strptime(timestring, "%a, %d %b %Y %H:%M:%S %Z")
        dt = pubDate.replace(tzinfo=ZoneInfo("Asia/Taipei")).astimezone(timezone.utc)
        datetime_string = dt.isoformat()
    elif publisher_name == 'ettoday':
        pubDate = datetime.strptime(timestring, "%a,%d %b %Y %H:%M:%S %z")
        dt = pubDate.replace(tzinfo=ZoneInfo("Asia/Taipei")).astimezone(timezone.utc)
        datetime_string = dt.isoformat()
    elif publisher_name == "cna":
        pubDate = datetime.strptime(timestring, "%a, %d %b %Y %H:%M:%S %z")
        dt = pubDate.replace(tzinfo=ZoneInfo("Asia/Taipei")).astimezone(timezone.utc)
        datetime_string = dt.isoformat()
    elif publisher_name == "rightplus":
        pubDate = datetime.strptime(timestring, "%a, %d %b %Y %H:%M:%S %z")
        dt = pubDate.replace(tzinfo=ZoneInfo("Asia/Taipei")).astimezone(timezone.utc)
        datetime_string = dt.isoformat()
    return datetime_string

### Based function of all the feeder, and you should overwrite abstract method if you inherit it
class Feeder(ABC):
  def __init__(self, media, batch_size: int=10):
    self.media = media
    self.batch_size = batch_size
  def export_gql(self, stories):
    '''
      export stories by gql
    '''
    register_adapter(dict, Json) # register_adapter to let psycopg can store dict as JSONB 
    MESH_GQL_ENDPOINT = os.environ['MESH_GQL_ENDPOINT']
    result = []
    gql_transport = RequestsHTTPTransport(url=MESH_GQL_ENDPOINT)
    gql_client = Client(transport=gql_transport, fetch_schema_from_transport=True)
    try:
      for i in range(0, len(stories), self.batch_size):
        stored_stories = stories[i: i+self.batch_size]
        json_data = gql_client.execute(gql(gql_mesh_create_stories), variable_values={"stories": stored_stories})
        result.append(json_data)
    except TransportError as e:
      print("Transport Error when exporting:", e)
    return result
  def export_postgresql(self, stories):
    '''
      export data by postgresql client
    '''
    conn = psycopg2.connect(
      database = os.environ['DB_NAME'], 
      user = os.environ['DB_USER'], 
      password = os.environ['DB_PASS'], 
      host = os.environ['DB_HOST'], 
      port = os.environ['DB_PORT'],
    )
    sql = '''
      INSERT INTO "Story" (source, title, url, full_content, summary, content, og_title, og_description, og_image, published_date, "createdAt", "apiData", origid)
      VALUES (%(source)s, %(title)s, %(url)s, %(full_content)s, %(summary)s, %(content)s, %(og_title)s, %(og_description)s, %(og_image)s, %(published_date)s, %(createdAt)s, %(apiData)s, %(origid)s)
      RETURNING id;
    '''
    success_urls = []
    try:
      for story in stories:
        story_data = {
          'source': int(story['source']['connect']['id']),
          'title': story['title'],
          'url': story['url'], ### url is unique key in postgres schema, you don't need to check it manually
          'full_content': '1' if story['full_content'] else '0',
          'summary': story['summary'],
          'content': story['content'],
          'og_title': story['og_title'],
          'og_description': story['og_description'],
          'og_image': story['og_image'],
          'published_date': story['published_date'],
          'createdAt': datetime.utcnow().strftime("%Y/%m/%d, %H:%M:%S"),
          'apiData': story.get('apiData', None),
          'origid': story.get('origid', 0),
        }
        try:
          with conn.cursor() as cur:
            cur.execute(sql, story_data)
            success_urls.append(story['url'])
        except psycopg2.Error as error: 
          print("Error while executing SQL statement:", error)
        conn.commit()
    finally:
      conn.close()
    return success_urls
  @abstractmethod
  def feed(self) -> list:
    '''
      abstract function to import outer data and processing
    '''
    pass

class RssFeeder(Feeder):
  def __init__(self, media, batch_size: int=10):
    super().__init__(media, batch_size)
  def feed(self) -> list:
    rss   = self.media['rss']
    title = self.media['title']
    media_id = self.media['id']
    publisher_name = config.publisher_en_mapping[title]
    print(f'Start parsing RSS of {title}...')
    ### parse rss
    feed = feedparser.parse(rss)
    ### deal with the feed.entries
    stories = []
    for entry in feed.entries:
        row = {}
        row['source'] = {
          "connect": {
            "id": media_id
          }
        }
        row['title'] = entry.title.replace("'", "\'")
        row['title'] = re.sub(r'<a href=.*?>(.+?)</a>', '\\1', row['title'])
        row['url']  = entry.link
        if row['url']:
            row['url'] = row['url'].replace('http://', 'https://', 1)
        row['full_content'] = False
        row['summary'] = entry.get('summary', '')
        row['content'] = entry.get('description', '')
        row['og_title'] = entry.get('title', '')
        row['og_description'] = ''
        row['og_image'] = ''

        ### some additional informations
        # row['author'] = entry.get('author', '')
        # row['category'] = entry.get('category', '')

        published_time = entry.get('published') if entry.get('published', '')!='' else entry.get('updated')
        row['published_date'] = parse_timestring(published_time, publisher_name)

        try:
            r = requests.get(row['url'])
            if r.status_code == 200:
                html_text = r.text
                soup = bs(html_text, features='lxml')
                head = soup.get('head', None)
                if head:
                    ogtitle = head.find_all('meta', {'property':'og:title'})
                    ogimage = head.find_all('meta', {'property':'og:image'})
                    ogdescription = head.find_all('meta', {'property':'og:description'})
                    pubdate = head.find_all('meta', {'property':'article:published_time'})
                    row['og_title'] = ogtitle[0]['content']
                    row['og_image'] = ogimage[0]['content']
                    row['og_description'] = ogdescription[0]['content']
                    if row['published_date'] == '' and pubdate:
                        row['published_date'] = parse_timestring(pubdate[1]['content'],  'twreporter')
        except Exception as e:
            print(e)
        stories.append(row)
    if len(stories)>0:
      print(f'Successfully parsing RSS of {title}...')
    else:
      print(f'Fail to parse RSS of {title}...')
    return stories

class ReadrFeeder(Feeder):
  def __init__(self, media, batch_size: int=10):
    super().__init__(media, batch_size)
  def feed(self) -> list:
    readr_url    = os.environ['READR_URL']
    gql_endpoint = os.environ['READR_GQL_ENDPOINT']
    readr_posts  = gql_query(gql_endpoint, gql_readr_posts.format(take=config.DEFAULT_READR_TAKE))

    stories = []
    print('Start parsing Readr...')
    for post in readr_posts['posts']:
        post_id = post['id']

        row = {}
        row['source'] = {
          "connect": {
            "id": self.media['id']
          }
        }
        row['title'] = post['name']
        row['published_date'] = post['publishTime']

        # publishTime = post['publishTime']
        image_url = post.get('heroImage', {}).get('resized', None).get('w800', '')
        if image_url == None:
            image_url = post.get('ogImage', {}).get('resized', {}).get('w800', '')

        row['origid'] = post_id
        row['url'] = readr_url + "/post/" + str(post_id)

        row['og_image'] = image_url
        row['og_title'] = post['name']
        ### parse summary
        summary = ''
        for block in post.get('summary', {}).get('blocks', []):
            summary += block.get('text', '')
        row['summary'] = summary
        row['og_description'] = summary

        ### parse conten, it should be stored as draftjs
        content = ''
        for block in post['content'].get('blocks', []):
            content += block.get('text', '')
        row['content'] = content
        row['apiData'] = json.dumps(post['apiData'])

        ### style: full_content determines whether the article is opened by webview or cms content
        scroll = re.search("embeddedCode", str(content))
        row['full_content'] = True if scroll else False
        row['full_screen_ad'] = 'none'
        stories.append(row)
    if len(stories)>0:
      print(f'Successfully parsing Readr...')
    else:
      print(f'Fail to parse Readr...')
    return stories
  
class ExternalFeeder(Feeder):
  def __init__(self, media, batch_size: int=10):
    ### Be careful that the media in ExternalFeeder is an array
    ### Send publisher_mm_externals to this field
    super().__init__(media, batch_size)
  def feed(self) -> list:
    '''
    In the feed function of ExternalFeeder, we need to send gql queries multiple times.
    As a result, instead of using  gql_fetch, we optimize all the queries processed in single connections
    '''
    publisher_mm_externals = self.media
    gql_endpoint = os.environ['MM_GQL_ENDPOINT'] 
    gql_transport = RequestsHTTPTransport(url=gql_endpoint)
    gql_client = Client(transport=gql_transport, fetch_schema_from_transport=True)

    ### organize the mm_partners
    mm_partners = gql_client.execute(gql(gql_mm_partners))
    mm_partners = mm_partners['partners']
    mm_partners_mapping = {}
    for partner in mm_partners:
        id = partner['id']
        slug = str(partner['slug']).lower()
        mm_partners_mapping[slug] = id

    ### process stories
    stories = []
    for publisher in publisher_mm_externals:
        publisher_en = publisher['rss']
        mm_partner_id = mm_partners_mapping[publisher_en]

        externals_string = gql_mm_externals.format(partner_id=mm_partner_id, take=config.DEFAULT_MM_EXTERNAL_TAKE)
        externals = gql_client.execute(gql(externals_string))
        externals = externals['externals']

        mesh_publisher_id = publisher['id']
        for external in externals:
            row = {}
            row['source'] = {
                "connect": {
                    "id": mesh_publisher_id
                }
            }
            row['title'] = external['title']
            row['published_date'] = external['publishedDate']
            row['origid'] = external['id']
            row['url'] = external['source']

            image_url = external['thumb']
            row['og_image'] = image_url
            row['og_title'] = external['title']
            row['summary'] = external['brief']
            row['og_description'] = external['brief']
            row['content'] = external['content']
            row['full_content'] = False
            row['full_screen_ad'] = 'none'
            stories.append(row)
    if len(stories)>0:
      print(f'Successfully parsing MirrorMedia external...')
    else:
      print(f'Fail to parse MirrorMedia external...')
    return stories
  
class MirrorMediaFeeder(Feeder):
  def __init__(self, media, batch_size: int=3):
    super().__init__(media, batch_size)
  def feed(self):
    gql_endpoint = os.environ['MM_GQL_ENDPOINT'] 
    mm_posts = gql_query(gql_endpoint, gql_mm_posts.format(take=config.DEFAULT_MM_TAKE))

    stories = []
    for post in mm_posts['posts']:
        post_id = post['id']
        slug  = post['slug']
        if len(slug)==0:
            continue

        row = {}
        row['source'] = {
          "connect": {
            "id": self.media['id']
          }
        }
        row['title'] = post['title']
        row['published_date'] = post['publishedDate']

        image_url = post.get('heroImage', {}).get('resized', {}).get('w800', '')
        row['origid'] = post_id
        row['url'] = self.media['official_site'] + "story/" + str(slug)

        row['og_image'] = image_url
        row['og_title'] = post['title']

        ### parse conten, it should be stored as draftjs
        content = ''
        for block in post['content'].get('blocks', []):
            content += block.get('text', '')
        row['content'] = content
        row['apiData'] = json.dumps(post['apiData'])

        ### parse summary
        summary = ''
        for block in post.get('brief', {}).get('blocks', []):
            summary += block.get('text', '')
        if summary=='':
            summary = content
        row['summary'] = summary
        row['og_description'] = summary

        ### style: full_content determines whether the article is opened by webview or cms content
        scroll = re.search("embeddedCode", str(content))
        row['full_content'] = True if scroll else False
        row['full_screen_ad'] = 'none'
        stories.append(row)
    if len(stories)>0:
      print(f'Successfully parsing RSS of MirrorMedia...')
    else:
      print(f'Fail to parse RSS of MirrorMedia...')
    return stories