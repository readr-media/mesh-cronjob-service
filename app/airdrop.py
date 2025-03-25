from google.cloud import bigquery as bq
from datetime import datetime, timezone
from app.gql import gql_query
import os
from app.tool import save_file
import math

def getUserPageview(db_name: str, table_name: str, start_time: str, end_time: str):
    '''
        Calculate user pageview, the example result is:
        {
            '186': {'homepage': 46, 'social': 15, 'media': 13, 'collection': 4},
            '19': {'homepage': 14, 'collection': 3},
        }
    '''
    QUERY = (
        f'SELECT jsonPayload.memberId AS memberId, jsonPayload.source AS source, COUNT(jsonPayload.source) AS view FROM `{db_name}.{table_name}` '
        'WHERE (resource.type="global") AND jsonPayload.type="pageview" AND (jsonPayload.memberId IS NOT null) AND (jsonPayload.memberId!="") '
        f'AND timestamp >= TIMESTAMP("{start_time}") AND timestamp <= TIMESTAMP("{end_time}") '
        'GROUP BY jsonPayload.memberId, jsonPayload.source ORDER BY memberId'
    )
    client = bq.Client()
    rows = client.query(QUERY).result()
    
    # Note: In log, media means newpage data
    focus_pv_source = ['media', 'homepage', 'social', 'collection']
    
    # parse response, pv_table contains {publisher_id: pv_count} relationship
    pv_table = {}
    for row in rows:
        if row.source not in focus_pv_source:
            continue
        pv_source = pv_table.setdefault(row.memberId, {})
        pv_source[row.source] = row.view
    return pv_table

def getMonthFollowing(gql_endpoint, start_time: str, end_time: str):
    format = "%Y-%m-%dT%H:%M:%S.%fZ" # which is used to compare datetime
    gql_following = '''
    query notifies{{
      notifies(where: {{type: {{equals: "follow"}}, createdAt: {{gt: "{start_time}" }} }}){{
        sender{{
          id
        }}
        member{{
          id
        }}
        createdAt 
      }}
    }}
    '''
    data = gql_query(gql_endpoint, gql_following.format(start_time = start_time))
    notifies = data['notifies']
    
    followed_table = {}
    for notify in notifies:
        try:
            senderId  = notify['sender']['id'] # the member who sent follow request
            followed  = notify['member']['id'] # the member who is followed
            createdAt = notify['createdAt']
            if (datetime.strptime(createdAt, format).astimezone(timezone.utc) > datetime.fromisoformat(end_time)):
                continue
            followed_list = followed_table.setdefault(followed, [])
            followed_list.append(senderId)
        except:
            print(f"Invalid format for notify: {notify}")
    return followed_table

def createAirdropTable(gql_endpoint, bq_db_name, bq_table_name, start_time, end_time, homepage_revenue, newpage_adsense_revenue, social_gam_revenue, collection_gam_revenue):
    '''
        Create airdrop table with {memberId, point} pair.
        Point is how many airdrop points each member will get during start_time to end_time.
    '''
    
    HOMEPAGE_NAME, SOCIALPAGE_NAME, NEWPAGE_NAME, COLLECTION_NAME = "homepage", "social", "media", "collection"
    
    # calculate the total_pv
    user_pv_table = getUserPageview(
        db_name = bq_db_name, 
        table_name = bq_table_name, 
        start_time = start_time, 
        end_time = end_time
    )
    homepage_pv_total   = sum([pv_source.get(HOMEPAGE_NAME, 0) for pv_source in user_pv_table.values()])
    socialpage_pv_total = sum([pv_source.get(SOCIALPAGE_NAME, 0) for pv_source in user_pv_table.values()])
    newpage_pv_total    = sum([pv_source.get(NEWPAGE_NAME, 0) for pv_source in user_pv_table.values()])
    collection_pv_total = sum([pv_source.get(COLLECTION_NAME, 0) for pv_source in user_pv_table.values()])

    # calculate followed table
    followed_table   = getMonthFollowing(gql_endpoint, start_time.replace('+00:00', 'Z'), end_time)
    total_followers = sum([len(set(followed)) for followed in followed_table.values()])
    if total_followers == 0:
        total_followers = 1 # avoid divide by 0 problem
    
    # calculate airdrop for each member
    airdrop_table = {}
    for memberId, pv_source in user_pv_table.items():
        homepage_pv   = pv_source.get(HOMEPAGE_NAME, 0)
        socialpage_pv = pv_source.get(SOCIALPAGE_NAME, 0)
        newpage_pv    = pv_source.get(NEWPAGE_NAME, 0)
        collection_pv = pv_source.get(COLLECTION_NAME, 0)
        followers     = followed_table.get(memberId, [])

        revenue = (
            (homepage_pv/homepage_pv_total)*homepage_revenue*0.3 +
            (newpage_pv/newpage_pv_total)*newpage_adsense_revenue*0.45 + 
            (socialpage_pv/socialpage_pv_total)*social_gam_revenue*0.85*0.3 +
            (collection_pv/collection_pv_total)*collection_gam_revenue*0.85*0.1 +
            (len(set(followers))/total_followers)*social_gam_revenue*0.2
        )
        revenue_point = math.floor(revenue)
        airdrop_table[memberId] = revenue_point
    
    # save the file
    date = datetime.now().strftime("%Y-%m-%d")  
    folder   = os.path.join("statements", "general", "json")
    filename = os.path.join(folder, f"monthly-airdrop-{date}.json")
    if not os.path.exists(folder):
        os.makedirs(folder)
    save_file(filename, airdrop_table)
    return filename