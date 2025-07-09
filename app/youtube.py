from googleapiclient.discovery import build
import os


def get_latest_videos_by_channel(channel_id, max_results=10):
    """
    使用 YouTube Data API v3 取得指定頻道的最新影片資料
    Args:
        channel_id (str): YouTube 頻道 ID
        max_results (int): 要取得的影片數量，預設 10
    Returns:
        list: 影片資料列表
    """
    api_key = os.environ.get('YOUTUBE_API_KEY')  # 請將 API 金鑰設在環境變數
    youtube = build('youtube', 'v3', developerKey=api_key)

    # 先取得 uploads playlist id
    channel_response = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    ).execute()

    uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    # 取得最新影片
    playlist_response = youtube.playlistItems().list(
        part='snippet,contentDetails',
        playlistId=uploads_playlist_id,
        maxResults=max_results
    ).execute()

    videos = []
    for item in playlist_response['items']:
        video_data = {
            'videoId': item['contentDetails']['videoId'],
            'title': item['snippet']['title'],
            'publishedAt': item['contentDetails']['videoPublishedAt'],
            'description': item['snippet']['description'],
            'thumbnails': item['snippet']['thumbnails']
        }
        videos.append(video_data)

    return videos


if __name__ == "__main__":
    channel_id = input("請輸入 YouTube 頻道 ID: ")
    max_results = input("請輸入要取得的影片數量（預設 10）: ")
    try:
        max_results = int(max_results) if max_results.strip() else 10
    except Exception:
        max_results = 10
    videos = get_latest_videos_by_channel(channel_id, max_results)
    print("\n=== 最新影片資料 ===")
    for v in videos:
        print(f"影片ID: {v['videoId']}")
        print(f"標題: {v['title']}")
        print(f"發布時間: {v['publishedAt']}")
        print(f"簡介: {v['description'][:60]}...")
        print(f"縮圖: {v['thumbnails'].get('default', {}).get('url', '')}")
        print("---")
