import mysql.connector.errors
import pandas
import pandas as pd
import streamlit as st
from _mysql_connector import MySQLInterfaceError as mysqlerror
from streamlit_option_menu import option_menu as op
from googleapiclient.discovery import  build
import pymongo
import mysql.connector as sql
import matplotlib.pyplot
import plotly.express as px
import sqlite3
from PIL import Image


client=pymongo.MongoClient("mongodb://localhost:27017/")
db=client['youtube']
mydb = sql.connect(host="localhost",
                   user="root",
                   password="Saranya@103",
                   database= "youtube_db"

                  )
mycursor = mydb.cursor(buffered=True)
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing",

                   page_icon= "https://tse1.mm.bing.net/th?id=OIP.lKZ7xlLixGofkpSrUWXbPgHaHa&pid=Api&P=0&h=180",
                   )

with st.sidebar:
    st.image("https://asset.kompas.com/crops/-iLJ6uSaELA5kI7umtqYw951peM=/303x27:1512x833/750x500/data/photo/2023/05/31/64773596ae8d1.png")
    selected = op(None, ["Home","Extract and Transform","View"])

#connecting youtube

key="AIzaSyDQ1bjgHnPksOXzfUGYrPXZFN8TJJY8ru4"
youtube=build('youtube','v3',developerKey=key)

# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id= channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_id = channel_id[i],
                    Channel_name = response['items'][i]['snippet']['title'],
                    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    Description = response['items'][i]['snippet']['description'],
                    Country = response['items'][i]['snippet'].get('country')
                    )
        ch_data.append(data)
    return(ch_data)


def get_channel_videos(channel_id):
    video_ids = []
    # get Uploads playlist id
    res = youtube.channels().list(id=channel_id,
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        res = youtube.playlistItems().list(playlistId=playlist_id,
                                           part='snippet',
                                           maxResults=50,
                                           pageToken=next_page_token).execute()

        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


# FUNCTION TO GET VIDEO DETAILS
def get_video_details(v_ids):
    video_stats = []

    for i in range(0, len(v_ids), 50):
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(v_ids[i:i + 50])).execute()
        for video in response['items']:
            video_details = dict(Channel_name=video['snippet']['channelTitle'],
                                 Channel_id=video['snippet']['channelId'],
                                 Video_id=video['id'],
                                 Title=video['snippet']['title'],
                                 Tags=video['snippet'].get('tags'),
                                 Thumbnail=video['snippet']['thumbnails']['default']['url'],
                                 Description=video['snippet']['description'],
                                 Published_date=video['snippet']['publishedAt'],
                                 Duration=video['contentDetails']['duration'],
                                 Views=video['statistics']['viewCount'],
                                 Likes=video['statistics'].get('likeCount'),
                                 Comments=video['statistics'].get('commentCount'),
                                 Favorite_count=video['statistics']['favoriteCount'],
                                 Definition=video['contentDetails']['definition'],
                                 Caption_status=video['contentDetails']['caption']
                                 )
            video_stats.append(video_details)
    return video_stats


# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        while True:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                     videoId=v_id,
                                                     maxResults=100,
                                                     pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id=cmt['id'],
                            Video_id=cmt['snippet']['videoId'],
                            Comment_text=cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author=cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date=cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count=cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count=cmt['snippet']['totalReplyCount']
                            )
                comment_data.append(data)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return comment_data


# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name


if selected == "Home":
    st.header("YOUTUBE DATA HARVESTING AND WAREHOUSING")
    st.subheader("Domain:")
    st.write("SocialMedia")
    st.subheader("Technologies used:")
    st.write("Python,MongoDB, Youtube Data API, MySql, Streamlit")
    st.subheader("Overview:")
    st.write("Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.""")

if selected == "Extract and Transform":
    tab1, tab2 = st.tabs(["$\huge EXTRACT $", "$\huge TRANSFORM $"])

    # EXTRACT TAB
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID below :")
        ch_id = st.text_input(
            "Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')

        if ch_id and st.button("Extract Data"):
            ch_details = get_channel_details(ch_id)
            #st.write(ch_details)
            df=pd.DataFrame(ch_details)
            st.write(df)



        if st.button("Upload to MongoDB"):
            with st.spinner('Please Wait for it...'):
                ch_details = get_channel_details(ch_id)
                v_ids = get_channel_videos(ch_id)

                vid_details = get_video_details(v_ids)


                def comments():
                    com_d = []
                    for i in v_ids:
                        com_d += get_comments_details(i)
                    return com_d

                comm_details = comments()

                collections1 = db.channel_details
                collections1.insert_many(ch_details)

                collections2 = db.video_details
                collections2.insert_many(vid_details)

                collections3 = db.comments_details
                collections3.insert_many(comm_details)
                st.success("Upload to MogoDB successful !!")
    # TRANSFORM TAB
    with tab2:
        st.markdown("#   ")
        st.markdown("### Select a channel to begin Transformation to SQL")

        ch_names = channel_names()
        user_inp = st.selectbox("Select channel", options=ch_names)


        def insert_into_channels():

            collections = db.channel_details
            query = """INSERT INTO channels VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""


            for i in collections.find({"Channel_name": user_inp}, {'_id': 0}):
                try:
                    mycursor.execute(query, tuple(i.values()))
                except mysqlerror:
                    pass

                except mysql.connector.errors.IntegrityError:
                    st.write("Channel already exist!!!")
                    st.stop()


        def insert_into_videos():
            collectionss = db.video_details
            #mycursor.execute(""" create table videos (Channel_name TEXT,Channel_id TEXT,Video_id TEXT,Title TEXT,Tags TEXT,Thumbnail TEXT,Description TEXT,Published_date TEXT,Duration TEXT,Views TEXT,Likes TEXT,Comments TEXT,Favorite_count TEXT,Definition TEXT,Caption_status TEXT);""")
            query1 = """INSERT INTO videos VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

            for i in collectionss.find({"Channel_name": user_inp}, {"_id": 0}):
                x=pandas.to_numeric(i['Views'])
                i['Views']=x.item()
                y = pandas.to_numeric(i['Likes'])
                i['Likes']=y.item()
                z = pandas.to_numeric(i['Comments'])
                i['Comments']=z.item()

                try:

                    for key in i:
                        #st.write(type(j))
                        if type(i[key]) is list:

                            i[key]=",".join([str(elem) for elem in i[key]])

                    t = tuple(i.values())
                    print(t)
                    mycursor.execute(query1, t)

                except Exception as e :
                    st.write(e)
                    st.stop()

        def insert_into_comments():
            collections1 = db.video_details
            collections2 = db.comments_details


            query2 = """INSERT INTO comments VALUES(%s,%s,%s,%s,%s,%s,%s)"""

            for vid in collections1.find({"Channel_name": user_inp}, {'_id': 0}):
                for i in collections2.find({'Video_id': vid['Video_id']}, {'_id': 0}):

                    t = tuple(i.values())
                    mycursor.execute(query2, t)



        if st.button("Submit"):


            insert_into_channels()
            insert_into_videos()
            insert_into_comments()
            mydb.commit()
            st.success("Transformation to MySQL Successful!!!")


# VIEW PAGE
if selected == "View":

    st.write("## :black[Select any question to get Insights]")
    questions = st.selectbox(" ",
                             ['Click the question that you would like to query',
                              'Display all videos name with channel name',
                              'videos count of each channel',
                              'Top 10 videos',
                              'Highest liked videos',
                              'Total number of views for channel',
                              'Highest Commented video'])

    if questions == 'Display all videos name with channel name':
        mycursor.execute(
            """SELECT Title AS Video_Title, channel_name AS Channel_Name FROM videos ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(), columns=list(mycursor.column_names))
        if len(df.index)>0:

            st.write(df)
        else:
            st.markdown("### :black[ Data not found!!!]")

    elif questions == 'videos count of each channel':
        mycursor.execute("""SELECT channel_name 
        AS Channel_Name, total_videos AS Total_Videos
                            FROM channels
                            ORDER BY total_videos DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)

        if len(df.index)>0:
            with st.container():
                col1, col2 = st.columns(2)
                with col1:
                    st.write(df)
                with col2:
                    fig = px.bar(df,
                                 x=mycursor.column_names[0],
                                 y=mycursor.column_names[1],
                                 orientation='v',
                                 color=mycursor.column_names[0]
                                 )
                    st.plotly_chart(fig, use_container_width=True)

        else:
            st.markdown("### :black[ Data not found!!!]")

    elif questions == 'Top 10 videos':
        mycursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, views AS Views 
                            FROM videos
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        if len(df.index)>0:
           st.write(df)
        else:
            st.markdown("### :black[ Data not found!!!]")
    elif questions == 'Highest liked videos':
        mycursor.execute("""SELECT channel_name AS Channel_Name,title AS Title,likes AS Likes_Count 
                            FROM videos
                            ORDER BY likes DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        if len(df.index)>0:
            with st.container():
                col1, col2 = st.columns(2)
                with col1:
                    st.write(df)
                with col2:
                    fig = px.bar(df,
                                 x=mycursor.column_names[0],
                                 y=mycursor.column_names[1],
                                 orientation='v',
                                 color=mycursor.column_names[0]
                                 )
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.markdown("### :black[ Data not found!!!]")
    elif questions == 'Total number of views for channel':
        mycursor.execute("""SELECT channel_name AS Channel_Name, views AS Views
                            FROM channels
                            ORDER BY views DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        if len(df.index)>0:
            with st.container():
                col1, col2 = st.columns(2)
                with col1:
                    st.write(df)
                with col2:
                    fig = px.bar(df,
                                 x=mycursor.column_names[0],
                                 y=mycursor.column_names[1],
                                 orientation='v',
                                 color=mycursor.column_names[0]
                                 )
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.markdown("### :black[ Data not found!!!]")
    elif questions == 'Highest Commented video':
        mycursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_ID,comments AS Comments
                            FROM videos
                            ORDER BY comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        if len(df.index)>0:
            with st.container():
                col1, col2 = st.columns(2)
                with col1:
                    st.write(df)
                with col2:
                    fig = px.bar(df,
                                 x=mycursor.column_names[0],
                                 y=mycursor.column_names[1],
                                 orientation='v',
                                 color=mycursor.column_names[0]
                                 )
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.markdown("### :black[ Data not found!!!]")

