from urllib import request
import pandas as pd
import pymongo
from googleapiclient.discovery import build
import pymongo
import mysql.connector

# MongoDB connection details (replace with your actual connection details)
mongo_uri = "mongodb+srv://santhosh:SONgokuSK27@youtubeproject.kxbhm6z.mongodb.net/"
client = pymongo.MongoClient(mongo_uri)
mongo_db = client.get_database("Youtube_project")  # Replace with your database name
collection = mongo_db.get_collection("Pokimane_Data")  # Replace with your collection name


api_key = 'AIzaSyBFUIMP4twedpPF4t2jPa_5xEibjvBrNPg'
channel_id = 'UChXKjLEzAB1K7EZQey7Fm1Q'
youtube = build('youtube', 'v3', developerKey=api_key)

def get_channel_videos(channel_id):
    channel_data = []
   
    request = youtube.channels().list(
      part="snippet,contentDetails,statistics",
      id=channel_id   
    )
    response = request.execute()
  
    for item in response["items"]:
        data={
           'Channel Name':item["snippet"]["title"],
           'Subcription':item["statistics"]["subscriberCount"],
           'Views':item["statistics"]["viewCount"],
           'Total Videos':item["statistics"]["videoCount"],
           'Playlist ID':item["contentDetails"]["relatedPlaylists"]["uploads"]
        }
        channel_data.append(data)
    return (pd.DataFrame(channel_data))

def get_video_ids(youtube, playlist_id):
    
    video_ids = []
    
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults = 50
    )
    response = request.execute()
    
    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])
        
    next_page_token = response.get('nextPageToken')
    while next_page_token is not None:
        request = youtube.playlistItems().list(
                    part='contentDetails',
                    playlistId = playlist_id,
                    maxResults = 50,
                    pageToken = next_page_token)
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')
        
    return video_ids
    
    
def get_video_details(youtube, video_ids):

    all_video_info = []
    
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute() 

        for video in response['items']:
            stats_to_keep = {
                'snippet': ['channelTitle','title','description','tags','publishedAt'],
                'statistics': ['viewCount','likeCount','favouriteCount','commentCount'],
                'contentDetails': ['duration','definition','caption']
            }
            
            video_info = {}
            video_info['video_id'] = video['id']

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v] = video[k][v]
                    except:
                        video_info[v] = None

            all_video_info.append(video_info)
    
    return pd.DataFrame(all_video_info)

def get_comments_in_video(youtube, channel_id, playlist_id):
    all_comments_info = []

    # Get the playlist ID for the channel's uploads
    uploads_playlist_id = playlist_id

    # Get video IDs from the uploads playlist
    video_ids = get_video_ids(youtube, uploads_playlist_id)

    for video_id in video_ids:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100,  # Adjust this if needed
            textFormat="plainText"
        )

        try:
            response = request.execute()
            if 'items' in response:
                for comment in response['items']:
                    comment_text = comment['snippet']['topLevelComment']['snippet']['textOriginal']
                    author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
                    published_at = comment['snippet']['topLevelComment']['snippet']['publishedAt']
                    video_title = get_video_details(youtube, [video_id])['title'].iloc[0]
                    
                    comment_info = {
                        'Video Title': video_title,
                        'Author': author,
                        'Published At': published_at,
                        'Comment': comment_text
                    }                  
                    all_comments_info.append(comment_info)
        except Exception as e:
            print(f"An error occurred while fetching comments for video {video_id}: {str(e)}")

    return pd.DataFrame(all_comments_info)

def store_data_in_mongodb(data_to_store):
    try:
        mongo_collection = mongo_db.get_collection("your_mongodb_collection_name")
        mongo_collection.insert_many(data_to_store)
        print("Data successfully stored in MongoDB")
    except Exception as e:
        print(f"An error occurred while storing data in MongoDB: {str(e)}")
        

# Call your functions to retrieve data
channel_data = get_channel_videos(channel_id)
video_ids = get_video_ids(youtube, channel_data['Playlist ID'].iloc[0])
video_details = get_video_details(youtube, video_ids)

# Store the retrieved data in MongoDB
store_data_in_mongodb(channel_data.to_dict('records'))
store_data_in_mongodb(video_details.to_dict('records'))

# Close the MongoDB client connection
client.close()

