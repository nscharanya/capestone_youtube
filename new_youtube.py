import googleapiclient.discovery
from pprint import pprint
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

def api_connect():
    
        api_key = "AIzaSyDZuSCpNoOxYBTlhoSTbr21-7E2zycAJns"
        youtube = googleapiclient.discovery.build(
                "youtube", "v3", developerKey =api_key)
        return youtube
youtube = api_connect()

# For channel_details
def get_channel_data(channel_id):
  request = youtube.channels().list(
          part="snippet,contentDetails,statistics",
          id=channel_id
      )
  response = request.execute()
  for i in response['items']:
    data1 = dict(Channel_name = i['snippet']['title'],
                Channel_Id = i['id'],
                Subscribers_Count = i['statistics']['subscriberCount'],
                Views = i['statistics']['viewCount'],
                Video_Count = i['statistics']['videoCount'],
                Channel_description = i['snippet']['description'],
                Playlist_Id = i['contentDetails']['relatedPlaylists']['uploads'],
                joined = i['snippet']['publishedAt'],
                thumbnails= i['snippet']['thumbnails']['default']['url'])
    return data1
  
# For video_ids 
def get_video_ids(channel_id):
  video_ids = []
  request = youtube.channels().list(
            part = "contentDetails",
            id = channel_id
  )
  response = request.execute()
  Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  next_page_token = None
  while True:
    request1 = youtube.playlistItems().list(
            part = "snippet",
            playlistId = Playlist_Id,
            maxResults = 50,
            pageToken = next_page_token)
    response1 = request1.execute()
    for i in range(len(response1['items'])):
      video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
    next_page_token = response1.get('nextPageToken')
    if next_page_token is None:
      break
  return video_ids

# get video data
def get_video_data(video_ids):
  video_data = [] 
  for video_id in video_ids:
    request = youtube.videos().list(
          part = "snippet,contentDetails,statistics",
          id = video_id)
    response = request.execute()
    for item in response['items']:
      data = dict(Channel_Name = item['snippet']['channelTitle'],
                  Channel_Id = item['snippet']['channelId'],
                  Video_Id = item['id'],
                  Title = item['snippet']['title'],
                  Tags = item['snippet'].get('tags'),                   # Not mandatory : returns tags if present else returns None(default value)
                  Thumbnails = item['snippet']['thumbnails']['default']['url'], 
                  Description = item['snippet'].get('description'),       # Video Description : Not mandatory
                  Published_Date = item['snippet']['publishedAt'],
                  Duration = item['contentDetails']['duration'],
                  Views =  item['statistics'].get('viewCount'),              # Not mandatory
                  Likes = item['statistics'].get('likeCount'),               # Not mandatory
                  Dislikes = item['statistics'].get('dislikeCount'),
                  Comments = item['statistics'].get('commentCount'),         # Not mandatory --> Comments are turned off sometimes
                  Favorite_Count = item['statistics'].get('favoriteCount'), # Not mandatory  --> Some videos might not be added into favourites
                  Definition =  item['contentDetails']['definition'],
                  Caption_Status = item['contentDetails']['caption'])
      video_data.append(data)
  return video_data

# get comment information
def get_comment_data(video_ids):
  Comment_data = []
  try:
      for video_id in video_ids:
        request = youtube.commentThreads().list(
            part = 'snippet',
            videoId = video_id,
            maxResults = 50)
        response = request.execute()
        for item in response['items']:
          data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                      Video_Id = item['snippet']['topLevelComment']['snippet']['videoId'],
                      Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                      Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                      Comment_Published = item['snippet']['topLevelComment']['snippet']['publishedAt'])
          Comment_data.append(data)
  except:
    pass
  return Comment_data


# get play_list details
def get_playlist_data(channel_id):
  Playlist_data = []
  next_page_token = None
  while True:
    request = youtube.playlists().list(
            part = "snippet,contentDetails",
            channelId = channel_id,
            maxResults = 50,
            pageToken = next_page_token

        )
    response = request.execute()
    for item in response['items']:
      data = dict(Playlist_Id = item['id'],
                  Title = item['snippet']['title'],
                  Channel_id = item['snippet']['channelId'],
                  Channel_name = item['snippet']['channelTitle'],
                  Published_At = item['snippet']['publishedAt'],
                  Video_Count = item['contentDetails']['itemCount'],
                  )
      Playlist_data.append(data)
    next_page_token = response.get('nextPageToken')
    if next_page_token is None:
      break
  return Playlist_data

# Upload to Mongodb - MongoDB Connection

client = pymongo.MongoClient("mongodb+srv://charanya_24:charanya@capestone-cluster.1inag8y.mongodb.net/?retryWrites=true&w=majority")
db = client["Youtube_data"]



def Channel_info(channel_id):
    ch_details = get_channel_data(channel_id)
    pl_details = get_playlist_data(channel_id)
    vi_ids = get_video_ids(channel_id)
    vi_details = get_video_data(vi_ids)
    com_details = get_comment_data(vi_ids)

    coll1 = db["channel_details"]  # Creating a collection named "channel_details"
    coll1.insert_one({"Channel_information": ch_details,
                      "Playlist_information": pl_details,
                      "Video_information": vi_details,
                      "Comment_information": com_details})

    return "upload completed successfully"

# Creation of channels table and inserting values
def channels_table(single_channel_name):
    mydb = psycopg2.connect(host = "localhost",
                                user = "postgres",
                                password = "Charanya@09",
                                database = "youtube_data",
                                port = 5432
                                )
    cursor = mydb.cursor()

    create_query = '''create table if not exists channels(
        Channel_name varchar(100),
        Channel_Id varchar(50) primary key,
        Subscribers_Count bigint,
        Views bigint,
        Video_Count int,
        Channel_description text,
        Playlist_Id varchar(50)
    )'''
    cursor.execute(create_query)
    mydb.commit()    # Saves the transaction

    single_channel_details=[]
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for channel_data in coll1.find({"Channel_information.Channel_name":single_channel_name},{"_id":0,"Channel_information":1}):
        single_channel_details.append(channel_data["Channel_information"])
    df_single_channel_details = pd.DataFrame(single_channel_details)

    # insert rows into table
    for index,row in df_single_channel_details.iterrows():
        insert_query = '''insert into channels (Channel_name,
                                                Channel_Id,
                                                Subscribers_Count,
                                                Views,
                                                Video_Count,
                                                Channel_description,
                                                Playlist_Id)
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_name'],
                row['Channel_Id'],
                row['Subscribers_Count'],
                row['Views'],
                row['Video_Count'],
                row['Channel_description'],
                row['Playlist_Id'])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            message = f"The provided channel name {single_channel_name} already exists "

            return message
        
# Creation of playlists table and inserting values
def playlists_table(single_channel_name):
    mydb = psycopg2.connect(host = "localhost",
                                user = "postgres",
                                password = "Charanya@09",
                                database = "youtube_data",
                                port = 5432
                                )
    cursor = mydb.cursor()

    create_query = '''create table if not exists playlists(
    Playlist_Id varchar(100) primary key,
    Title varchar(100),
    Channel_id varchar(100),
    Channel_name varchar(100),
    Published_At timestamp,
    Video_Count int
    )'''
    cursor.execute(create_query)
    mydb.commit()    # Saves the transaction

    single_playlist_details=[]
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for channel_data in coll1.find({"Channel_information.Channel_name":single_channel_name},{"_id":0}):
        single_playlist_details.append(channel_data["Playlist_information"])
    df_single_playlist_details = pd.DataFrame(single_playlist_details[0])

    for index,row in df_single_playlist_details.iterrows():
            insert_query = '''insert into playlists (Playlist_Id,
                                                    Title,
                                                    Channel_id,
                                                    Channel_name,
                                                    Published_At,
                                                    Video_Count)
                                                    values(%s,%s,%s,%s,%s,%s)'''
            values =   (row['Playlist_Id'],
                        row['Title'],
                        row['Channel_id'],
                        row['Channel_name'],
                        row['Published_At'],
                        row['Video_Count'])
            try:
                cursor.execute(insert_query,values)
                mydb.commit()
            except:
                print("Playlist values already inserted")
                
# Creation of videos table and inserting values
def videos_table(single_channel_name):
    mydb = psycopg2.connect(host = "localhost",
                                user = "postgres",
                                password = "Charanya@09",
                                database = "youtube_data",
                                port = 5432
                                )
    cursor = mydb.cursor()
  
    create_query = '''create table if not exists videos(Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Video_Id varchar(40) primary key,
                                                        Title varchar(150),
                                                        Tags text,                   
                                                        Thumbnails varchar(200), 
                                                        Description text,       
                                                        Published_Date timestamp,
                                                        Duration interval,
                                                        Views bigint,              
                                                        Likes bigint, 
                                                        Dislikes bigint,           
                                                        Comments int,        
                                                        Favorite_Count int, 
                                                        Definition varchar(20),
                                                        Caption_Status varchar(50))'''
    cursor.execute(create_query)
    mydb.commit()    # Saves the transaction
    
    single_video_details=[]
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for channel_data in coll1.find({"Channel_information.Channel_name":single_channel_name},{"_id":0}):
        single_video_details.append(channel_data["Video_information"])
    df_single_video_details = pd.DataFrame(single_video_details[0])

    for index,row in df_single_video_details.iterrows():
            insert_query = '''insert into videos (Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Tags,                   
                                                Thumbnails, 
                                                Description,       
                                                Published_Date,
                                                Duration,
                                                Views,              
                                                Likes,   
                                                Dislikes,           
                                                Comments,        
                                                Favorite_Count, 
                                                Definition,
                                                Caption_Status)
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            values =   (row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnails'],
                        row['Description'],
                        row['Published_Date'],
                        row['Duration'],
                        row['Views'],
                        row['Likes'],
                        row['Dislikes'],
                        row['Comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_Status'])
            try:
                cursor.execute(insert_query,values)
                mydb.commit()
            except:
                print("Videos values already inserted")
                
# Creation of comments table and inserting values
def comments_table(single_channel_name):
    mydb = psycopg2.connect(host = "localhost",
                                user = "postgres",
                                password = "Charanya@09",
                                database = "youtube_data",
                                port = 5432
                                )
    cursor = mydb.cursor()
    
    create_query = '''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp)'''
    cursor.execute(create_query)
    mydb.commit()    # Saves the transaction
    
    single_comment_details=[]
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for channel_data in coll1.find({"Channel_information.Channel_name":single_channel_name},{"_id":0}):
          single_comment_details.append(channel_data["Comment_information"])
    df_single_comment_details = pd.DataFrame(single_comment_details[0])

    for index,row in df_single_comment_details.iterrows():
            insert_query = '''insert into comments(Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published)
                                                    values(%s,%s,%s,%s,%s)'''
            values =   (row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published'])
            

            try:
                    cursor.execute(insert_query,values)
                    mydb.commit()
            except:
                    print("Comments values already inserted")

def tables(single_channel):
    message = channels_table(single_channel)
    if message:
        return message
    else:
        playlists_table(single_channel)
        videos_table(single_channel)
        comments_table(single_channel)
        return "Tables created successfully"
def show_channels_table():
    ch_list=[]
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"Channel_information":1}):
        ch_list.append((ch_data["Channel_information"]))
    df = st.dataframe(ch_list)

    return df
def show_playlists_table():
    pl_list=[]
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"Playlist_information":1}):
        for i in range(len(pl_data["Playlist_information"])):
            pl_list.append((pl_data["Playlist_information"][i]))
    df1 = st.dataframe(pl_list)

    return df1
def show_videos_table():
    vi_list=[]
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"Video_information":1}):
        for i in range(len(vi_data["Video_information"])):
            vi_list.append((vi_data["Video_information"][i]))
    df2 = st.dataframe(vi_list)

    return df2
def show_comments_table():
    com_list=[]
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"Comment_information":1}):
        for i in range(len(com_data["Comment_information"])):
            com_list.append((com_data["Comment_information"][i]))
    df3 = st.dataframe(com_list)

    return df3

# Streamlit part
st.set_page_config(page_title="Welcome to YouTube Data Analysis", layout="wide")

st.title("YouTube Data Analytics")
st.markdown("---")
st.subheader("Main Highlights")
st.markdown("- Python Scripting")
st.markdown("- Data Retrieval")
st.markdown("- MongoDB Integration")
st.markdown("- API Utilization")
st.markdown("- Data Management (MongoDB and SQL)")
st.markdown("---") 

st.title("Welcome to YouTube Data Analysis")
col1, col2 = st.columns(2)
with col1:
    channel_id = st.text_input("Enter the Channel ID")

    if st.button("Collect and store data"):
        channel_ids=[]
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        for ch_data in coll1.find({},{"_id":0,"Channel_information":1}):
            channel_ids.append((ch_data["Channel_information"]["Channel_Id"]))

        if channel_id in channel_ids:
            st.success("Channel Details of the given channel_id already exists")
        else:
            message_placeholder = st.empty()  # Create an empty placeholder
            message_placeholder.info("Data collection initiated. Please wait...")  # Display the initial message
            insert = Channel_info(channel_id)   # Calls Channel_info function
            message_placeholder.success(insert)  # Replace the initial message with the success message


    all_channel_names = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for channel_data in coll1.find({},{"_id":0,"Channel_information":1}):
        all_channel_names.append(channel_data["Channel_information"]["Channel_name"])

    unique_channel = st.selectbox("Select a Channel to Explore",all_channel_names)



    if st.button("Migrate to SQL"):
        Table = tables(unique_channel)       # Calls tables function
        st.success(Table)

    with st.expander("Show Tables"):
        show_table = st.radio("Select a Table",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))
        # User's selection is stored in Show_table variable
        if show_table == "CHANNELS":
            show_channels_table()       # Calls show_channels_table function
        elif show_table =="PLAYLISTS":
            show_playlists_table()      # Calls show_playists_table function
        elif show_table == "VIDEOS":
            show_videos_table()         # Calls show_videos_table function
        elif show_table == "COMMENTS":
            show_comments_table()       # Calls show_comments_table function


# SQL Connection
mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "Charanya@09",
                            database = "youtube_data",
                            port = 5432
                            )
cursor = mydb.cursor()
with col2:
    with st.expander("SQL Queries"):
        question = st.selectbox("Select your question",("1. What are the names of all the videos and their corresponding channels?",
                                                    "2. Which channels have the most number of videos, and how many videos do they have?",
                                                    "3. What are the top 10 most viewed videos and their respective channels?",
                                                    "4. How many comments were made on each video, and what are their corresponding video names?",
                                                    "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                    "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                                    "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                                    "8. What are the names of all the channels that have published videos in the year 2022?",
                                                    "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                    "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))
        if question == "1. What are the names of all the videos and their corresponding channels?":
            query1 = '''select title as video_name, channel_name as channelname from videos'''
            cursor.execute(query1)
            mydb.commit()
            t1 = cursor.fetchall()
            df1= pd.DataFrame(t1,columns = ['video_name','channelname'])
            st.write(df1)
            
        elif question == "2. Which channels have the most number of videos, and how many videos do they have?":
            query2 = '''select channel_name as channelname, video_count as num_of_videos 
                        from channels order by video_count desc;'''
            cursor.execute(query2)
            mydb.commit()
            t2 = cursor.fetchall()
            df2= pd.DataFrame(t2,columns = ['channelname','number_of_videos'])
            st.write(df2)

        elif question == "3. What are the top 10 most viewed videos and their respective channels?":
            query3 = '''select title as video_name, views as no_of_views,channel_name as channelname 
                        from videos where views is not null order by views desc limit(10);'''
            cursor.execute(query3)
            mydb.commit()
            t3 = cursor.fetchall()
            df3= pd.DataFrame(t3,columns = ['video_name','number_of_views','channelname'])
            st.write(df3)

        elif question == "4. How many comments were made on each video, and what are their corresponding video names?":
            query4 = '''select title as video_name, comments as no_of_comments 
                        from videos where comments is not null;'''
            cursor.execute(query4)
            mydb.commit()
            t4 = cursor.fetchall()
            df4= pd.DataFrame(t4,columns = ['video_name','number_of_comments'])
            st.write(df4)

        elif question == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
            query5 = '''select title as video_name, likes as no_of_likes,channel_name as channelname 
                        from videos where likes is not null order by likes desc'''
            cursor.execute(query5)
            mydb.commit()
            t5 = cursor.fetchall()
            df5= pd.DataFrame(t5,columns = ['video_name','number_of_likes','channelname'])
            st.write(df5)

        elif question == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
            query6 = '''select title as video_name, likes as no_of_likes, dislikes as no_of_dislikes
                        from videos ;'''
            cursor.execute(query6)
            mydb.commit()
            t6 = cursor.fetchall()
            df6= pd.DataFrame(t6,columns = ['video_name','number_of_likes','number_of_dislikes'])
            st.write(df6)

        elif question == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
            query7 = '''select channel_name as channelname, views as total_views from channels;'''
            cursor.execute(query7)
            mydb.commit()
            t7 = cursor.fetchall()
            df7= pd.DataFrame(t7,columns = ['channelname','total_views'])
            st.write(df7)

        elif question == "8. What are the names of all the channels that have published videos in the year 2022?":
            query8 = '''select channel_name as channelname, title as video_name, published_date 
                        from videos where extract(year from published_date)=2022;'''
            cursor.execute(query8)
            mydb.commit()
            t8 = cursor.fetchall()
            df8= pd.DataFrame(t8,columns = ['channelname','video_name','published_date'])
            st.write(df8)

        elif question == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
            query9 = '''select channel_name as channelname, avg(duration) as average_duration  
                        from videos group by channel_name;'''
            cursor.execute(query9)
            mydb.commit()
            t9 = cursor.fetchall()
            df9 = pd.DataFrame(t9,columns = ['channelname','average_duration'])
            st.write(df9)
            
        elif question == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
            query10 = '''select title as video_name, comments as no_of_comments, channel_name as channelname 
                        from videos where comments is not null order by comments desc;'''
            cursor.execute(query10)
            mydb.commit()
            t10 = cursor.fetchall()
            df10 = pd.DataFrame(t10,columns = ['video_name','no_of_comments','channelname'])
            st.write(df10)
