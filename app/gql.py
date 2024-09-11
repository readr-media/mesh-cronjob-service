from gql.transport.requests import RequestsHTTPTransport
from gql.transport.exceptions import TransportError
from gql import gql, Client

from datetime import datetime, timedelta
import pytz

def gql_query(gql_endpoint, gql_string: str, gql_variables: str=None, operation_name: str=None):
  json_data = None
  try:
    gql_transport = RequestsHTTPTransport(url=gql_endpoint)
    gql_client = Client(transport=gql_transport,
                        fetch_schema_from_transport=True)
    json_data = gql_client.execute(gql(gql_string), variable_values=gql_variables, operation_name=operation_name)
  except Exception as e:
    print("GQL query error:", e)
  return json_data

def gql_fetch_latest_stories(gql_endpoint, days: int):
    ### calculate start time
    current_time = datetime.now(pytz.timezone('Asia/Taipei'))
    start_time = current_time - timedelta(days=days)
    formatted_start_time = start_time.isoformat()
    
    ### fetch stories
    all_stories = gql_query(gql_endpoint, gql_mesh_latest_stories.format(START_PUBLISHED_DATE=formatted_start_time))
    all_stories = all_stories['stories']
    return all_stories
  
def gql_fetch_media_statistics(gql_endpoint, days: int):
    ### calculate start time
    current_time = datetime.now(pytz.timezone('Asia/Taipei'))
    start_time = current_time - timedelta(days=days)
    formatted_start_time = start_time.isoformat()
    
    ### fetch stories
    all_stories = gql_query(gql_endpoint, gql_mesh_media_statistics.format(START_PUBLISHED_DATE=formatted_start_time))
    all_stories = all_stories['stories']
    return all_stories

def get_most_like_comment(gql_endpoint, story_id):
    story = gql_query(gql_endpoint, gql_story_comments.format(STORY_ID=story_id))
    comments = story['story'].get('comment', [])
    if len(comments)==0:
      return {}
    most_like_comment = sorted(comments, key=lambda comment: comment.get('likeCount', 0), reverse=True)[0]
    return most_like_comment

### GQL Query: Please follow the convention of gql_[product]_[list]
gql_mesh_publishers = '''
query Publishers{
  publishers{
    id
    title
    rss
    official_site
    source_type
    full_content
    full_screen_ad
    sponsoredCount
  }
}
'''

gql_mesh_publishers_sponsor = '''
query Publishers{
  publishers{
    id
    title
    customId
    sponsoredCount
  }
}
'''

# Open information for publishers, which is used by frontend
gql_mesh_publishers_open = '''
query Publishers{
  publishers {
    id
    customId
    title
    logo
    followerCount
    description
  }
}
'''

### TODO: sponsorCount should be modified to real data after connecting cashflow
gql_mesh_sponsor_publishers = '''
query Publishers{
  publishers{
    id
    title
    official_site
    logo
    full_content
    paywall
    sponsorCount: sponsoredCount
  }
}
'''

gql_mm_posts = '''
  query {{
    posts( where: {{state: {{equals: "published" }}, }}, take: {take}, orderBy: [{{id:desc}}] )
    {{
      id
      style
      slug
      title
      publishedDate
      heroImage {{
        resized {{
          w800
        }}
      }}
      brief
      content
      apiData
    }}
  }}
'''

gql_readr_posts = '''
query {{
posts(
  where: {{ state: {{equals: "published"}}, publishTime: {{not: null}} }}, take: {take}, orderBy: [{{id:desc}}]
)
  {{
    id
    name
    style
    summary
	  content
    publishTime
    heroImage {{
      resized {{
        w800
      }}
    }}
    ogImage {{
      resized {{
        w800
      }}
    }}
    apiData
  }}
}}
'''

gql_mesh_create_stories = '''
mutation CreateStories($stories: [StoryCreateInput!]!){
    createStories(data: $stories){
    	id
    	title
    	url
  	}
}
'''

gql_mm_partners = '''
query {
    partners{
        id
        name
        slug
    }
}
'''

gql_mm_externals = '''
query Externals{{
  externals(
      where:{{ partner: {{ id: {{ equals: {partner_id} }} }} }},
      orderBy: {{ id: desc }}, take: {take}
  )
  {{
      id
      source
      title
      brief
      content
      publishedDate
      thumb
  }}
}}
'''

# Example START_PUBLISHED_DATAE: "2024-06-01T17:30:15+05:30"
# Published date should follow the format of ISO8601
# category id greater than 0 is used to filter the stories without being categorized
gql_mesh_latest_stories = '''
query Stories{{
  stories(
    where: {{
      published_date: {{
        gte: "{START_PUBLISHED_DATE}"
      }},
      category: {{
        id: {{
          gt: 0
        }}
      }}
    }},
    orderBy: {{
      published_date: desc
    }},
  ){{
    id
    url
    title
    category{{
      id
      slug
    }}
    source{{
      id
      title
      customId
    }}
    published_date
    summary
    og_title
    og_image
    og_description
    full_content
    origid
    picksCount: pickCount(
      where: {{
        kind: {{
          equals: "read"
        }},
        is_active: {{
          equals: true
        }}
      }}
    )
    picks: pick(
      where: {{
        kind: {{
          equals: "read"
        }},
        is_active: {{
          equals: true
        }}
      }}
      take: 5
    ){{
      createdAt
      member{{
        id
        name
        avatar
      }}
    }}
    commentCount
    paywall
    full_screen_ad
  }}
}}
'''

gql_mesh_media_statistics = '''
query Stories{{
  stories(
    where: {{
      published_date: {{
        gte: "{START_PUBLISHED_DATE}"
      }},
      category: {{
        id: {{
          gt: 0
        }}
      }}
    }},
  ){{
    source{{
      id
    }}
    readsCount: pickCount(
      where: {{
        kind: {{
          equals: "read"
        }},
        is_active: {{
          equals: true
        }}
      }}
    )
  }}
}}
'''

gql_mesh_sponsor_stories = '''
query stories($where: StoryWhereInput!){
  stories(
    where: $where
  ){
    id
    url
    title
    published_date
  	og_title
    og_image
    og_description
    category{
      slug
    }
    picksCount: pickCount(
      where: {
        kind: {
          equals: "read"
        },
        is_active: {
          equals: true
        }
      }
    )
    picks: pick(
      where: {
        kind: {
          equals: "read"
        },
        is_active: {
          equals: true
        }
      }
    ){
      createdAt
      member{
        id
        name
        avatar
      }
    }
    commentCount
    paywall
    full_screen_ad
    source{
      id
      title
      customId
    }
  }
}
'''

gql_check_exist_stories = '''
query Stories($where: StoryWhereInput!){
  stories(
    where: $where
  ){
    url
  }
}
'''

gql_recent_stories_pick = '''
query Story{{
  stories(where: {{source: {{id: {{equals: {ID} }} }} }}, orderBy: {{ id: desc }}, take: {TAKE}){{
    id
    title
    url
    summary
    picks: pick(
      where: {{
        kind: {{
          equals: "read"
        }},
        is_active: {{
          equals: true
        }}
      }}
    ){{
      createdAt
      member{{
        id
        name
        avatar
      }}
    }}
    pickCount: pickCount(
      where: {{
        kind: {{
          equals: "read"
        }},
        is_active: {{
          equals: true
        }}
      }}
    )
    commentCount
    og_title
    og_image
    og_description
    full_content
    paywall
    isMember
    published_date
    full_screen_ad
  }}
}}
'''

gql_recent_stories_comment = '''
query Story{{
  stories(where: {{source: {{id: {{equals: {ID} }} }} }}, orderBy: {{ id: desc }}, take: {TAKE}){{
    id
    title
    url
    summary
    commentCount: commentCount(
      where: {{
        is_active: {{
          equals: true
        }}
      }}
    )
    og_title
    og_image
    og_description
    full_content
    paywall
    isMember
    published_date
    full_screen_ad
  }}
}}
'''

gql_readr_info = '''
query Publishers{
  publishers(where: {title: {equals: "READr" } }){
    id
    title
    customId
  }
}
'''

gql_recent_reads = '''
query Picks{{
  picks(where: {{kind: {{equals: "read"}}, is_active: {{ equals: true }} }}, orderBy: {{id: desc}}, take: {TAKE}){{
    story{{
      id
    }}
  }}
}}
'''

gql_single_story = '''
query Story{{
    story(where: {{id: {ID} }}){{
        id
        title
        url
        summary
        picks: pick(
          where: {{
            kind: {{
              equals: "read"
            }},
            is_active: {{
              equals: true
            }}
          }},
          take: 5
        ){{
          createdAt
          member{{
            id
            name
            avatar
          }}
        }}
        pickCount: pickCount(
          where: {{
            kind: {{
              equals: "read"
            }},
            is_active: {{
              equals: true
            }}
          }},
        )
        source{{
          id
          title
          customId
        }}
        commentCount
        og_title
        og_image
        og_description
        full_content
        paywall
        isMember
        published_date
        full_screen_ad
    }}
}}
'''

### This is only used to calcuate likeCount number of each comment
gql_comment_statistic = '''
query Comments{{
  comments(
    where: {{
      like: {{some: {{}} }},
      story: {{ NOT: {{}} }},
      is_active: {{equals: true}}
    }}, 
    orderBy: {{id: desc}}, 
    take: {TAKE}
  ){{
    id
    likeCount: likeCount(where: {{
      is_active: {{
        equals: true
      }}
    }})
  }}
}}
'''

### reveal the detail information about comment
# Note: This operation use lots of relation data, don't pass in too many CommentWhereInput elements
gql_comment_detail = '''
query Comments($where: CommentWhereInput!){
  comments(
    where: $where
  ){
    id
    member{
      id
      name
      avatar
    }
    content
    story{
      id
      title
      source{
        id
        title
        customId
      }
      published_date
    }
    likeCount: likeCount(where: {
      is_active: {
        equals: true
      }
    })
  }
}
'''

### Get all the comments of a story
gql_story_comments = '''
query Story{{
  story(where: {{id: {STORY_ID} }}){{
    comment(where: {{
      is_active: {{
        equals: true
      }}
    }})
    {{
      id
      content
      member{{
        id
        name
        avatar
      }}
      likeCount(where: {{
        is_active: {{
          equals: true
        }}
      }})
    }}
  }}
}}
'''