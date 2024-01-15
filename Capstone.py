from googleapiclient.discovery import build
import pymongo
from pymongo import MongoClient
import mysql.connector
import pandas as pd
import time
import datetime
import streamlit as st
import re

#API Collection
def API_Connect():
    API_ID="AIzaSyDK3PDqmEWn5JNjxQB2PhSLkRKKe12Cu4s"
    api_service_name="youtube"
    api_version="v3"
    yt=build(api_service_name,api_version,developerKey=API_ID)
    return yt

youtube=API_Connect()

#Channel information
def channel_details(channelid):
    request = youtube.channels().list(
                      part = "snippet,ContentDetails,statistics",
                      id = channelid
    )
    response=request.execute()
    
    for i in response['items']:
        data = dict(Channel_Name=i['snippet']['title'],
                    Channel_ID=i['id'],
                    Channel_Description=i['snippet']['description'],
                    Subscribers_Count=i['statistics']['subscriberCount'],
                    Views=i['statistics']['viewCount'],
                    Total_Videos=i['statistics']['videoCount'],
                    Play_list_ID=i['contentDetails']['relatedPlaylists']['uploads']
                    )
    return data

#video IDs
def video_ids(channelid):
    request = youtube.channels().list(id=channelid,part = "ContentDetails")
    response=request.execute()
    Playlist_ID=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    video_IDs=[]
    npt=None
    while True:
        request1=youtube.playlistItems().list(part = 'snippet',playlistId=Playlist_ID,maxResults=50,pageToken=npt)
        response1=request1.execute()
        
        for i in range(len(response1['items'])):
            video_IDs.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        npt = response1.get('nextPageToken')
        if npt is None:
            break
    return video_IDs

#video information
def video_details(IDs_Video):

    video_datas=[]
    
    for vid in IDs_Video:
        request=youtube.videos().list(
                      part = "snippet,ContentDetails,statistics",
                      id = vid
        )
        response=request.execute()
    
        for i in response["items"]:
            data=dict(Channel_Name=i['snippet']['channelTitle'],
                      Channel_ID=i['snippet']['channelId'],
                      Video_ID=i['id'],
                      Video_Title=i['snippet']['title'],
                      Thumbnail=i['snippet']['thumbnails']['default']['url'],
                      Description=i['snippet'].get('description'),
                      PublishedAt=i['snippet']['publishedAt'],
                      Duration=i['contentDetails']['duration'],
                      Views=i['statistics'].get('viewCount'),
                      Likes=i['statistics'].get('likeCount'),
                      Comments=i['statistics'].get('commentCount'),
                      Favouite_Count=i['statistics']['favoriteCount'],
                      Definition=i['contentDetails']['definition'],
                      Caption=i['contentDetails']['caption'],
                      Tags=i['snippet'].get('tags')
                     )
            video_datas.append(data)
    return video_datas

#Comment information
def comment_details(IDs_Video):
    
    comment_info=[]
    try:
        for vid in IDs_Video:
            request = youtube.commentThreads().list(
                 part="snippet",
                 videoId=vid,
                 maxResults=50
            )
            response=request.execute()
            
            for i in response['items']:
                data=dict(Comment_Id=i['snippet']['topLevelComment']['id'],
                          Video_ID=i['snippet']['videoId'],
                          Comment=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                          Commented_By=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                          Commented_At=i['snippet']['topLevelComment']['snippet']['updatedAt'])
                
                comment_info.append(data)
    except:
        pass

    return comment_info

# Playlists Information

def playlist_details(channelid):

    playlist_info=[]
    npt=None
    while True:
        request=youtube.playlists().list(
                   part='snippet,contentDetails',
                   channelId=channelid,
                   maxResults=50,
                   pageToken=npt
        )
        response=request.execute()
        
        for i in response['items']:
            data=dict(Playlists_ID=i['id'],
                      Title=i['snippet']['title'],
                      Channel_ID=i['snippet']['channelId'],
                      Channel_Name=i['snippet']['channelTitle'],
                      PublishedAt=i['snippet']['publishedAt'],
                      Videos_Count=i['contentDetails']['itemCount'])
            playlist_info.append(data)
        npt=response.get('nextPageToken')
        if npt is None:
            break


    return playlist_info

connection = MongoClient("mongodb+srv://Nivethini_K_S:Dhansh@atlascluster.cs2pteg.mongodb.net/")
connection

def youtube_data(channelid):
    details_of_channel=channel_details(channelid)
    IDs_Video=video_ids(channelid)
    details_of_playlist=playlist_details(channelid)
    details_of_video=video_details(IDs_Video)
    details_of_comment=comment_details(IDs_Video)
    data = {'Channel Details':details_of_channel,'Playlist Details':details_of_playlist,'Video Details':details_of_video,'Comment Details':details_of_comment}

    collection = db["Channel Informations"]
    collection.insert_one(data)

    return "uploaded"

#create table for channels in sql
def Channels():
    sqlconnection = mysql.connector.connect(
                  host='localhost',
                  user='root',
                  password='12345678',
                  database='YouTube_Data'
    )
    cursor=sqlconnection.cursor()
    
    query4='''drop table if exists YouTube_Channel'''
    cursor.execute(query4)
    sqlconnection.commit()
    
    try:
    
        query1= '''create table if not exists YouTube_Channel(Channel_Name varchar(100),
                                                               Channel_ID varchar(100) primary key,
                                                               Channel_Description text,
                                                               Subscribers_Count bigint,
                                                               Views bigint,
                                                               Total_Videos int,
                                                               Play_list_ID varchar(100))'''
        cursor.execute(query1)
        sqlconnection.commit()
    
    
    except:
        print("table already created")
                                                           
    
    Channels_list=[]
    db=connection["Youtube_Data"]
    collection = db["Channel Informations"]
    for ch_data in collection.find({},{"_id":0,"Channel Details":1}):
        Channels_list.append(ch_data['Channel Details'])
    df=pd.DataFrame(Channels_list)
    
    
    for i,r in df.iterrows():
        query2='''insert into YouTube_Channel(Channel_Name,
                                              Channel_ID,
                                              Channel_Description,
                                              Subscribers_Count,
                                              Views,
                                              Total_Videos,
                                              Play_list_ID)
    
                                              values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(r['Channel_Name'],
                r['Channel_ID'],
                r['Channel_Description'],
                r['Subscribers_Count'],
                r['Views'],
                r['Total_Videos'],
                r['Play_list_ID'])
        try:
            cursor.execute(query2,values)
            sqlconnection.commit()
        except:
            print("channels already inserted")

def Playlists():
    sqlconnection = mysql.connector.connect(
              host='localhost',
              user='root',
              password='12345678',
              database='YouTube_Data'
    )
    cursor=sqlconnection.cursor()
    
    query4='''drop table if exists ChannelPlaylists'''
    cursor.execute(query4)
    sqlconnection.commit()
    
    
    
    query1= '''create table if not exists ChannelPlaylists(Playlists_ID varchar(100) primary key,
                                                            Title varchar(100),
                                                            Channel_ID varchar(100),
                                                            Channel_Name varchar(100),
                                                            PublishedAt timestamp,
                                                            Videos_Count int)'''
                                                           
    cursor.execute(query1)
    sqlconnection.commit()

    Playlists_list=[]
    db=connection["Youtube_Data"]
    collection = db["Channel Informations"]
    for pl_data in collection.find({},{"_id":0,"Playlist Details":1}):
        for i in range(len(pl_data['Playlist Details'])):
            Playlists_list.append(pl_data['Playlist Details'][i])
    df1=pd.DataFrame(Playlists_list)
    
    from datetime import datetime
                                       
    for i,r in df1.iterrows():
        query2='''insert into ChannelPlaylists(Playlists_ID,
                                                Title,
                                                Channel_ID,
                                                Channel_Name,
                                                PublishedAt,
                                                Videos_Count
                                                )
    
                                              values(%s,%s,%s,%s,%s,%s)'''
    
        r['PublishedAt']=r['PublishedAt'].replace("T"," ")
        r['PublishedAt']=r['PublishedAt'].replace("Z","")
        r['PublishedAt']=datetime.strptime(r['PublishedAt'],"%Y-%m-%d %H:%M:%S")
        
        values=(r['Playlists_ID'],
                r['Title'],
                r['Channel_ID'],
                r['Channel_Name'],
                r['PublishedAt'],
                r['Videos_Count'])
        
        cursor.execute(query2,values)
        sqlconnection.commit()

def Comments():
    sqlconnection = mysql.connector.connect(
              host='localhost',
              user='root',
              password='12345678',
              database='YouTube_Data'
    )
    cursor=sqlconnection.cursor()
    
    query4='''drop table if exists Channel_Comments'''
    cursor.execute(query4)
    sqlconnection.commit()
    
    
    
    query1= '''create table if not exists Channel_Comments(Comment_Id varchar(100) primary key,
                                                           Video_ID varchar(50),
                                                           Comment text,
                                                           Commented_By varchar(150),
                                                           Commented_At timestamp
                                                           )'''
                                                           
    cursor.execute(query1)
    sqlconnection.commit()

    Comments_list=[]
    db=connection["Youtube_Data"]
    collection = db["Channel Informations"]
    for co_data in collection.find({},{"_id":0,"Comment Details":1}):
        for i in range(len(co_data['Comment Details'])):
            Comments_list.append(co_data['Comment Details'][i])
    df3=pd.DataFrame(Comments_list)
    
    from datetime import datetime
                                       
    for i,r in df3.iterrows():
        query2='''insert into Channel_Comments(Comment_Id,
                                               Video_ID,
                                               Comment,
                                               Commented_By,
                                               Commented_At)
    
                                              values(%s,%s,%s,%s,%s)'''
    
        r['Commented_At']=r['Commented_At'].replace("T"," ")
        r['Commented_At']=r['Commented_At'].replace("Z","")
        r['Commented_At']=datetime.strptime(r['Commented_At'],"%Y-%m-%d %H:%M:%S")
        
        values=(r['Comment_Id'],
                r['Video_ID'],
                r['Comment'],
                r['Commented_By'],
                r['Commented_At'])
        
        cursor.execute(query2,values)
        sqlconnection.commit()

def Videos():
    sqlconnection = mysql.connector.connect(
                  host='localhost',
                  user='root',
                  password='12345678',
                  database='YouTube_Data'
    )
    cursor=sqlconnection.cursor()
    
    query4='''drop table if exists Channel_Videos'''
    cursor.execute(query4)
    sqlconnection.commit()
    
    
    
    query1= '''create table if not exists Channel_Videos(Channel_Name varchar(100),
                                                         Channel_ID varchar(100),
                                                         Video_ID varchar(50) primary key,
                                                         Video_Title varchar(150),
                                                         Thumbnail varchar(200),
                                                         Description text,
                                                         PublishedAt text,
                                                         Duration time,
                                                         Views bigint,
                                                         Likes bigint,
                                                         Comments int,
                                                         Favouite_Count int,
                                                         Definition varchar(10),
                                                         Caption varchar(50)
                                                         )'''
                                                           
    cursor.execute(query1)
    sqlconnection.commit()

    Videos_list=[]
    db=connection["Youtube_Data"]
    collection = db["Channel Informations"]
    for vi_data in collection.find({},{"_id":0,"Video Details":1}):
        for i in range(len(vi_data['Video Details'])):
            Videos_list.append(vi_data['Video Details'][i])
    df2=pd.DataFrame(Videos_list)

    def convert_duration(duration):
        time_pattern = re.compile(r'PT(\d+H)?(\d+M)?(\d+S)?')
        match = time_pattern.match(duration)
        if match:
            hours = int(match.group(1)[0:-1]) if match.group(1) else 0
            minutes = int(match.group(2)[0:-1]) if match.group(2) else 0
            seconds = int(match.group(3)[0:-1]) if match.group(3) else 0
            return f"{hours:02}:{minutes:02}:{seconds:02}"
        else:
            return None

    df2['Duration'] = df2['Duration'].astype(str)  # Convert to string format
    df2['Duration'] = df2['Duration'].apply(convert_duration)
    
    for i,r in df2.iterrows():
        query2='''insert into Channel_Videos(Channel_Name,
                                             Channel_ID,
                                             Video_ID,
                                             Video_Title,
                                             Thumbnail,
                                             Description,
                                             PublishedAt,
                                             Duration,
                                             Views,
                                             Likes,
                                             Comments,
                                             Favouite_Count,
                                             Definition,
                                             Caption)
    
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    
        #r['PublishedAt']=r['PublishedAt'].replace("T"," ")
        #r['PublishedAt']=r['PublishedAt'].replace("Z","")
        #r['PublishedAt']=datetime.strptime(r['PublishedAt'],"%Y-%m-%d %H:%M:%S")
        
        values=(r['Channel_Name'],
                r['Channel_ID'],
                r['Video_ID'],
                r['Video_Title'],
                r['Thumbnail'],
                r['Description'],
                r['PublishedAt'],
                r['Duration'],
                r['Views'],
                r['Likes'],
                r['Comments'],
                r['Favouite_Count'],
                r['Definition'],
                r['Caption']
                )
        
        cursor.execute(query2,values)
        sqlconnection.commit()

def tables():
    Channels()
    Playlists()
    Comments()
    Videos()

    return "Done"

def View_Channel_Table():
    Channels_list=[]
    db=connection["Youtube_Data"]
    collection = db["Channel Informations"]
    for ch_data in collection.find({},{"_id":0,"Channel Details":1}):
        Channels_list.append(ch_data['Channel Details'])
    df=st.dataframe(Channels_list)

    return df

def View_Playlists_Table():
    Playlists_list=[]
    db=connection["Youtube_Data"]
    collection = db["Channel Informations"]
    for pl_data in collection.find({},{"_id":0,"Playlist Details":1}):
        for i in range(len(pl_data['Playlist Details'])):
            Playlists_list.append(pl_data['Playlist Details'][i])
    df1=st.dataframe(Playlists_list)

    return df1

def View_Comment_Table():
    Comments_list=[]
    db=connection["Youtube_Data"]
    collection = db["Channel Informations"]
    for co_data in collection.find({},{"_id":0,"Comment Details":1}):
        for i in range(len(co_data['Comment Details'])):
            Comments_list.append(co_data['Comment Details'][i])
    df3=st.dataframe(Comments_list)

    return df3

def View_Videos_Table():
    Videos_list=[]
    db=connection["Youtube_Data"]
    collection = db["Channel Informations"]
    for vi_data in collection.find({},{"_id":0,"Video Details":1}):
        for i in range(len(vi_data['Video Details'])):
            Videos_list.append(vi_data['Video Details'][i])
    df4=st.dataframe(Videos_list)

    return df4

#streamlit

with st.sidebar:
    st.title(":green[Nivethini's Capstone 1: YouTube Data Harversting And Warehousing]")
    st.header("Skills that I taken away")
    st.caption("API integration")
    st.caption("Python scripting")
    st.caption("Data Collection")
    st.caption("Streamlit")
    st.caption("Data Management using MongoDB (Atlas) and SQL")

channelid=st.text_input("Enter Channel ID")

if st.button("Collect the data and Store in MongoDB"):
    ch_ids=[]
    db=connection["Youtube_Data"]
    collection = db["Channel Informations"]
    for ch_data in collection.find({},{"_id":0,"Channel Details":1}):
        ch_ids.append(ch_data['Channel Details']['Channel_ID'])

    if channelid in ch_ids:
        st.success("Given Channel ID already inserted")

    else:
        insert=youtube_data(channelid)
        st.success(insert)

if st.button("Transfer to MySQL"):
    tble=tables()
    st.success(tble)

view_table = st.radio("Please Select the Table",("Channel","Playlist","Videos","Comments"))

if view_table == "Channel":
    View_Channel_Table()
elif view_table == "Playlist":
    View_Playlists_Table()
elif view_table == "Videos":
    View_Videos_Table()
elif view_table == "Comments":
    View_Comment_Table()


#SQL Connection

sqlconnection = mysql.connector.connect(
              host='localhost',
              user='root',
              password='12345678',
              database='YouTube_Data'
)
cursor=sqlconnection.cursor()

question=st.selectbox("Select the Question",("What are the names of all the videos and their corresponding channels?",
                                             "Which channels have the most number of videos, and how many videos do they have?",
                                             "What are the top 10 most viewed videos and their respective channels?",
                                             "How many comments were made on each video, and what are their corresponding video names?",
                                             "Which videos have the highest number of likes, and what are their corresponding channel names?",
                                             "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                             "What is the total number of views for each channel, and what are their corresponding channel names?",
                                             "What are the names of all the channels that have published videos in the year 2022?",
                                             "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                             "Which videos have the highest number of comments, and what are their corresponding channel names?"))
    
#question1
if question=="What are the names of all the videos and their corresponding channels?":
    ques1='''select Video_Title as videos, Channel_Name as ChannelName from Channel_Videos'''
    cursor.execute(ques1)
    ques1_data=[]
    for data in cursor.fetchall():
        ques1_data.append(data)
    pd.DataFrame(ques1_data)
    fd1=pd.DataFrame(ques1_data,columns=["Video_Title","Channel_Name"])
    st.write(fd1)

#question2
elif question=="Which channels have the most number of videos, and how many videos do they have?":
    ques2='''select Channel_Name as ChannelName,Total_Videos as totalvideos from YouTube_Channel order by Total_Videos desc;'''
    cursor.execute(ques2)
    ques2_data=[]
    for data in cursor.fetchall():
        ques2_data.append(data)
    pd.DataFrame(ques2_data)
    fd2=pd.DataFrame(ques2_data,columns=["Channel_Name","Total_Videos"])
    st.write(fd2)

#question3
elif question=="What are the top 10 most viewed videos and their respective channels?":
    ques3='''select Views as views , Channel_Name as ChannelName,Video_Title as VideoTitle from Channel_Videos
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(ques3)
    ques3_data=[]
    for data in cursor.fetchall():
        ques3_data.append(data)
    pd.DataFrame(ques3_data)
    fd3=pd.DataFrame(ques3_data,columns=["Views","Channel_Name","Video_Title"])
    st.write(fd3)

#question4
elif question=="How many comments were made on each video, and what are their corresponding video names?":
    ques4='''select Comments as No_comments ,Video_Title as VideoTitle from Channel_Videos'''
    cursor.execute(ques4)
    ques4_data=[]
    for data in cursor.fetchall():
        ques4_data.append(data)
    pd.DataFrame(ques4_data)
    fd4=pd.DataFrame(ques4_data,columns=["Number of Comments","Video_Title"])
    st.write(fd4)

#question5
elif question=="Which videos have the highest number of likes, and what are their corresponding channel names?":
    ques5='''select Video_Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from Channel_Videos order by Likes desc;'''
    cursor.execute(ques5)
    ques5_data=[]
    for data in cursor.fetchall():
        ques5_data.append(data)
    pd.DataFrame(ques5_data)
    fd5=pd.DataFrame(ques5_data,columns=["Video_Title","Channel_Name","Likes"])
    st.write(fd5) 

#question6
elif question=="What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    ques6='''select Likes as likeCount,Video_Title as VideoTitle from Channel_Videos;'''
    cursor.execute(ques6)
    ques6_data=[]
    for data in cursor.fetchall():
        ques6_data.append(data)
    pd.DataFrame(ques6_data)
    fd6=pd.DataFrame(ques6_data,columns=["Likes","Video_Title"])
    st.write(fd6)

#question7
elif question=="What is the total number of views for each channel, and what are their corresponding channel names?":
    ques7='''select Channel_Name as ChannelName, Views as Channelviews from YouTube_Channel;'''
    cursor.execute(ques7)
    ques7_data=[]
    for data in cursor.fetchall():
        ques7_data.append(data)
    pd.DataFrame(ques7_data)
    fd7=pd.DataFrame(ques7_data,columns=["Channel_Name","Views"])
    st.write(fd7)

#question8
elif question=="What are the names of all the channels that have published videos in the year 2022?":
    ques8='''select Video_Title as VideoTitle, PublishedAt as VideoRelease, Channel_Name as ChannelName from Channel_Videos
                where extract(year from PublishedAt) = 2022;'''
    cursor.execute(ques8)
    ques8_data=[]
    for data in cursor.fetchall():
        ques8_data.append(data)
    pd.DataFrame(ques8_data)
    fd8=pd.DataFrame(ques8_data,columns=["Video_Title","Published At","Channel_Name"])
    st.write(fd8)

#question9
elif question == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    ques9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM Channel_Videos GROUP BY Channel_Name;"
    cursor.execute(ques9)
    ques9_data=[]
    for data in cursor.fetchall():
        ques9_data.append(data)
    pd.DataFrame(ques9_data)
    fd9=pd.DataFrame(ques9_data,columns=["Channel_Name","Average_Duration"])
    T9=[]
    for index, row in fd9.iterrows():
        channel_title = row['Channel_Name']
        average_duration = row['Average_Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel_Name": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))    

#question10
elif question=="Which videos have the highest number of comments, and what are their corresponding channel names?":
    ques10='''select Video_Title as VideoTitle, Channel_Name as ChannelName, Comments as comments from Channel_Videos order by Comments desc;'''
    cursor.execute(ques10)
    ques10_data=[]
    for data in cursor.fetchall():
        ques10_data.append(data)
    pd.DataFrame(ques10_data)
    fd10=pd.DataFrame(ques10_data,columns=["Video_Title","Channel_Name","No.of Comments"])
    st.write(fd10)

