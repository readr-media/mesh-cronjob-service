publisher_en_mapping = {
    '報導者': 'twreporter',
    '公共電視': 'pts',
    'READr': 'readr',
    '東森新聞': 'ettoday',
    '鏡週刊': 'mirrormedia',
}

### cache-control
upload_configs = {
    "real_time": "no-store",
    "cache_control_short": 'max-age=30',
    "cache_control_long": 'max-age=50',
    "cache_control": 'max-age=86400',
    "content_type_json": 'application/json',
    }

DEFAULT_GQL_TTL = 3600
DEFAULT_CATEGORY_LATEST_GQL_DAYS = 2
DEFAULT_CATEGORY_LATEST_TTL = 3600

### for cronjob
DEFAULT_MOST_FOLLOWER_NUM = 5
DEFAULT_MOST_READ_MEMBER_NUM = 5
DEFAULT_MOST_READ_MEMBER_DAYS = 7
DEFAULT_MOST_READ_STORY_DAYS = 1
DEFAULT_MEDIA_STATISTICS_DAYS = 7
DEFAULT_MOST_READ_STORY_NUM = 10
DEFAULT_MOST_SPONSOR_STORY_DAYS = 2
DEFAULT_MOST_SPONSOR_PUBLISHER_NUM = 5
DEFAULT_RECENT_READR_DAYS = 7
MOST_PICKCOUNT_PUBLISHER_NUM = 3
HOTPAGE_SPONSOR_PUBLISHER_NUM = 3
HOTPAGE_SPONSOR_PUBLISHER_STORY_NUM =3
HOTPAGE_POPULAR_STORY_READS_NUM = 1000
HOTPAGE_MOST_LIKE_COMMENTS_NUM = 4 # How many comments to reveal on hotpage
HOTPAGE_RECENT_COMMENTS_NUM = 100 # How many comments to fetch likeCount recently
PUBLISHER_STORIES_NUM = 240

### take number for each feeeder
DEFAULT_READR_TAKE = 3
DEFAULT_MM_EXTERNAL_TAKE = 20
DEFAULT_MM_TAKE = 10