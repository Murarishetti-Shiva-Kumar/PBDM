import os
import matplotlib
import matplotlib.pyplot as plt
import spark as spark
from datetime import datetime
from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from shapely.geometry import Point
from textblob import TextBlob

import geopandas as gpd
from geopandas import GeoDataFrame

matplotlib.use('Agg')

spark = SparkSession.builder.appName("Tweet Data Analysis").getOrCreate()

df = spark.read.json(r"E:\engg\Master\PBDM\tweetsdata.txt")
df.createOrReplaceTempView("tweets")
users = df.select("user.*").dropDuplicates(subset=['id'])
users.createOrReplaceTempView("user")
entities = df.select("entities.*")
entities.createOrReplaceTempView("entity")
retweeted = df.select("retweeted_status.*")
retweeted.createOrReplaceTempView("user_retweeted")
retweeted.printSchema()

# Query1
# Query to fetch geo coordinates such as latitude & longitude of user
def get_query1():
    plt.clf()
    # os.remove("Query1.csv")
    # os.remove("graph1.png")
    query_1 = spark.sql(
        "SELECT user.name,user.screen_name,user.id,geo.coordinates[0] As latt,geo.coordinates[1] As long from tweets where geo is not null")
    pd = query_1.toPandas()
    pd.to_csv('Query1.csv', index=False)
    geometry = [Point(xy) for xy in zip(pd['long'], pd['latt'])]
    gdf = GeoDataFrame(pd, geometry=geometry)
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    gdf.plot(ax=world.plot(), marker='o', color='red', markersize=15)
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title("Geo Locations of Corona Tweets")
    plt.tight_layout()
    # plt.show()
    plt.savefig('graph1.png')
    graph1 = BytesIO()
    plt.savefig(graph1)
    graph1.seek(0)
    return graph1

# Query2
def get_query2():
    plt.clf()
    # os.remove("Query2.csv")
    # os.remove("graph2.png")
    query_2 = spark.sql(
        "select name, followers_count from user where name is not null and verified=='true' order by followers_count DESC LIMIT 20")
    pd = query_2.toPandas()
    pd.to_csv('Query8.csv', index=False)
    pd.plot.pie(y="followers_count", labels=pd.name.tolist(), autopct='%1.2f%%',shadow=False, legend=False, fontsize=8)
    plt.title("Followers Count of verified Users")
    plt.axis('equal')
    plt.tight_layout()
    # plt.show()
    plt.savefig('graph2.png')
    graph2 = BytesIO()
    plt.savefig(graph2)
    graph2.seek(0)
    return graph2

# Query3
# Query to fetch 20 most influential person based on retweeted count
def get_query3():
    plt.clf()
    # os.remove("Query3.csv")
    # os.remove("graph3.png")
    query_3 = spark.sql(
        "select  t.Name,t.Retweeted_Count from(select user.screen_name as Name,SUM(retweet_count) as Retweeted_Count, count(*) from user_retweeted where user.screen_name is not null group by Name ) t order by Retweeted_Count DESC LIMIT 20")
    pd = query_3.toPandas()
    pd.to_csv('Query3.csv', index=False)
    x = pd.Name.tolist()
    y = pd.Retweeted_Count.tolist()
    plt.stackplot(x, y)
    plt.title("Most Influential Person Based on Retweet")
    plt.xlabel('User Name')
    plt.ylabel('Retweeted count')
    plt.xticks(rotation=45, ha="right")
    # plt.axis('equal')
    plt.tight_layout()
    # plt.show()
    plt.savefig('graph3.png')
    graph3 = BytesIO()
    plt.savefig(graph3)
    graph3.seek(0)
    return graph3

# Query4
# Query to fetch top 20 trending hashtags
def get_query4():
    plt.clf()
    # os.remove("Query4.csv")
    # os.remove("graph4.png")
    query4 = spark.sql(
        "SELECT LOWER(hashtags.text) As Hashtags, COUNT(*) AS total_count FROM tweets LATERAL VIEW EXPLODE(entities.hashtags) AS hashtags GROUP BY LOWER(hashtags.text) ORDER BY total_count DESC LIMIT 20")
    pd = query4.toPandas()
    pd.to_csv('Query4.csv', index=False)
    plt.plot(pd.Hashtags.tolist(), pd.total_count.tolist())
    plt.xticks(rotation=45, ha="right")
    plt.title("Trending Hashtags")
    plt.xlabel('Hashtags')
    plt.ylabel('Count')
    # plt.axis('equal')
    plt.tight_layout()
    # plt.show()
    plt.savefig("graph4.png")
    graph4 = BytesIO()
    plt.savefig(graph4)
    graph4.seek(0)
    return graph4

# Query5
# Query to perform sentiment analysis on tweet based on keywords 'covid','coronavirus'
def get_query5():
    plt.clf()
    # os.remove("graph5.png")
    date = df.select("created_at")

    def dateMTest(dateval):
        dt = datetime.datetime.strptime(dateval, '%a %b %d %H:%M:%S +0000 %Y')
        return dt

    d = udf(dateMTest, DateType())
    df1 = df.withColumn("created_date", d(date.created_at))
    df1.createOrReplaceTempView("Tweet_Sentiment")
    query_5 = spark.sql(
        "SELECT id,text,created_date  FROM Tweet_Sentiment WHERE 1=1 AND (upper(text) LIKE '%COVID%' OR text LIKE '%coronavirus%' or text like '%covid%' or upper(text) like '%CORONAVIRUS%')")
    i = 0
    positive = 0
    neutral = 0
    negative = 0
    for t in query_5.select("text").collect():
        i = i + 1
        # print("It is ",i,str(t.text))
        analysis = TextBlob(str((t.text).encode('ascii', 'ignore')))
        # print(analysis.sentiment.polarity)
        if (analysis.sentiment.polarity < 0):
            negative = negative + 1
            # print(i, " in negative")
        elif (analysis.sentiment.polarity == 0.0):
            neutral = neutral + 1
            # print(i, " in neutral")
        elif (analysis.sentiment.polarity > 0):
            positive = positive + 1
            # print(i, " in positive")
    # print("Total negative % is", ((negative) * 100) / i)
    # print("Total neutral % is", ((neutral) * 100) / i)
    # print("Total positive % is", ((positive) * 100) / i)
    negative_percent = ((negative) * 100) / i
    positive_percent = ((positive) * 100) / i
    neutral_percent = ((neutral) * 100) / i
    size_of_groups = [negative_percent, positive_percent, neutral_percent]
    names = 'negative_percent', 'positive_percent', 'neutral_percent'
    plt.bar(x=names, height=size_of_groups, color=['red', 'green', 'blue'])
    plt.title("Sentiment Analysis on Tweet")
    # plt.axis('equal')
    plt.tight_layout()
    # plt.show()
    plt.savefig('graph5.png')
    graph5 = BytesIO()
    plt.savefig(graph5)
    graph5.seek(0)
    return graph5

# Query6
# Query to fetch top 10 languages
def get_query6():
    plt.clf()
    # os.remove("Query6.csv")
    # os.remove("graph6.png")
    query_6 = spark.sql(
        "select lang, count(*) as count from tweets where lang is not null group by lang order by count desc limit 10")
    pd = query_6.toPandas()
    pd.to_csv('Query6.csv', index=False)
    pd.plot.pie(y="count", labels=pd.lang.tolist(), autopct='%1.2f%%', shadow=False, legend=False, fontsize=8)
    plt.title("Top Languages Used in Profile")
    plt.axis('equal')
    plt.tight_layout()
    # plt.show()
    plt.savefig("graph6.png")
    graph6 = BytesIO()
    plt.savefig(graph6)
    graph6.seek(0)
    return graph6

# Query7
# Query to fetch tweeted devices count
def get_query7():
    plt.clf()
    # os.remove("Query7.csv")
    # os.remove("graph7.png")
    qandriod = spark.sql(
        "select 'Andriod' as DeviceName, count(user.name) as TweetsCount from tweets where source LIKE '%and%'")
    qiphone = spark.sql(
        "select 'Iphone' as DeviceName, count(user.name) as TweetsCount from tweets where source LIKE '%iphone%' ")
    qipad = spark.sql(
        "select 'Ipad' as DeviceName, count(user.name) as TweetsCount from tweets where source LIKE '%ipad%' ")
    qwebpage = spark.sql(
        "select 'Web App' as DeviceName, count(user.name) as TweetsCount from tweets where source LIKE '%Web App%' ")
    query_7 = qandriod.union(qiphone).union(qipad).union(qwebpage)
    pd = query_7.toPandas()
    pd.to_csv("Query7.csv", index=False)
    plt.bar(x=pd.DeviceName.tolist(), height=pd.TweetsCount.tolist())
    plt.title("Devices Used for Tweets")
    plt.xlabel('Source')
    plt.ylabel('TweetsCount')
    # plt.axis('equal')
    plt.tight_layout()
    # plt.show()
    plt.savefig("graph7.png")
    graph7 = BytesIO()
    plt.savefig(graph7)
    graph7.seek(0)
    return graph7

# Query8
# Query to fetch number of tweets per day
def get_query8():
    plt.clf()
    # os.remove("Query8.csv")
    # os.remove("graph8.png")
    query_8 = spark.sql(
        "select count(*) as TweetCount,SUBSTR(user.created_at,0,10) as TWEETDATE from tweets where created_at IS not null GROUP BY user.created_at ORDER BY TweetCount DESC LIMIT 30")
    pd = query_8.toPandas()
    pd.to_csv('Query5.csv', index=False)
    y = pd.TweetCount.tolist()
    x = pd.TWEETDATE.tolist()
    plt.plot(x, y, alpha=0.5)
    plt.title("Stats of Tweets Per Day")
    plt.xlabel('Tweet Date')
    plt.ylabel('Tweet Count')
    plt.xticks(rotation=45, ha="right")
    # plt.axis('equal')
    plt.tight_layout()
    # plt.show()
    plt.savefig('graph8.png')
    graph8 = BytesIO()
    plt.savefig(graph8)
    graph8.seek(0)
    return graph8

# Query9
# Query to fetch users profile_use_background_image: true or false
def get_query9():
    plt.clf()
    # os.remove("Query9.csv")
    # os.remove("graph9.png")
    query_9 = spark.sql(
        "select sum(img_count) as img_count,profile_use_background_image as background_img from (select count(*) as img_count,profile_use_background_image from user where profile_use_background_image is not null group by profile_use_background_image union select count(*) as img_count,user.profile_use_background_image as profile_use_background_image from user_retweeted where user.profile_use_background_image is not null group by user.profile_use_background_image) group by profile_use_background_image")
    pd = query_9.toPandas()
    pd.to_csv('Query9.csv', index=False)
    # pd.set_index("background_img", drop=True, inplace=True)
    pd.plot.pie(y="img_count", labels=pd.background_img.tolist(), autopct='%1.2f%%', fontsize=10)
    my_circle = plt.Circle((0, 0), 0.5, color='white')
    p = plt.gcf()
    p.gca().add_artist(my_circle)
    plt.title("User's Profile Background")
    plt.axis('equal')
    plt.tight_layout()
    # plt.show()
    plt.savefig('graph9.png')
    graph9 = BytesIO()
    plt.savefig(graph9)
    graph9.seek(0)
    return graph9

# Query10
# Query to get the users with top 20 highest number of friends
def get_query10():
    plt.clf()
    # os.remove("Query10.csv")
    # os.remove("graph10.png")
    query_10 = spark.sql(
        "select name,friends_count as friends from user order by friends desc limit 20")
    pd = query_10.toPandas()
    pd.to_csv('Query10.csv', index=False)
    x = pd.name.tolist()
    y = pd.friends.tolist()
    plt.plot(x, y, alpha=0.5)
    plt.title("Trends on Number of friends")
    plt.xlabel('User Name')
    plt.ylabel('Friends Count')
    plt.xticks(rotation=45, ha="right")
    # plt.axis('equal')
    plt.tight_layout()
    # plt.show()
    plt.savefig("graph10.png")
    graph10 = BytesIO()
    plt.savefig(graph10)
    graph10.seek(0)
    return graph10

# Query11
# Query to get most mentioned twitter accounts
def get_query11():
    plt.clf()
    # os.remove("Query11.csv")
    # os.remove("graph11.png")
    query_11 = spark.sql(
        "select usermention, count(*) as value from entity lateral view explode(entity.user_mentions.name) as usermention group by usermention order by value desc limit 20")
    pd = query_11.toPandas()
    pd.to_csv('Query11.csv', index=False)
    matplotlib.rcParams['figure.figsize'] = 15, 5
    x = pd.usermention.tolist()
    y = pd.value.tolist()
    plt.plot(x, y)
    plt.title("Most Mentioned Twitter Accounts")
    plt.xlabel('Tweeted User')
    plt.ylabel('User mentioned count')
    plt.xticks(rotation=45, ha="right")
    # plt.axis('equal')
    plt.tight_layout()
    # plt.show()
    plt.savefig('graph11.png')
    graph11 = BytesIO()
    plt.savefig(graph11)
    graph11.seek(0)
    return graph11

# Query12
# Query to fetch users geo location enabled: true or false
def get_query12():
    plt.clf()
    # os.remove("Query12.csv")
    # os.remove("graph12.png")
    query_12 = spark.sql(
        "select sum(geo_count) as geo_count,geo_enabled from (select count(*) as geo_count,geo_enabled from user where geo_enabled is not null group by geo_enabled union select count(*) as geo_count,user.geo_enabled as geo_enabled from user_retweeted where user.geo_enabled is not null group by user.geo_enabled) group by geo_enabled")
    pd = query_12.toPandas()
    pd.to_csv('Query12.csv', index=False)
    pd.plot.pie(y="geo_count", labels=pd.geo_enabled.tolist(), autopct='%1.2f%%', fontsize=10)
    my_circle = plt.Circle((0, 0), 0.5, color='white')
    p = plt.gcf()
    p.gca().add_artist(my_circle)
    plt.title("Geo Location Settings Enabled for Account")
    plt.axis('equal')
    plt.tight_layout()
    # plt.show()
    plt.savefig("graph12.png")
    graph12 = BytesIO()
    plt.savefig(graph12)
    graph12.seek(0)
    return graph12

# Query13
# Query to fetch maximum length tweet string based on keywords from text tag
def get_query13():
    plt.clf()
    # os.remove("Query13.csv")
    # os.remove("graph13.png")
    qcovid = spark.sql(
        "select 'covid' as keyword, count(*) as Count from tweets where text like '%covid%'")
    qpandemic = spark.sql(
        "select 'pandemic' as keyword, count(*) as Count from tweets where text like '%pandemic%'")
    qmask = spark.sql(
        "select 'masks' as keyword , count(*) as Count from tweets where text like '%masks%'")
    qlock = spark.sql(
        "select 'lockdown' as keyword, count(*) as Count from tweets where text like '%lockdown%'")
    qstayhome = spark.sql(
        "select 'stayhome' as keyword, count(*) as Count from tweets where text like '%stayhome%'")
    query_13 = qcovid.union(qpandemic).union(qmask).union(qlock).union(qstayhome)
    pd = query_13.toPandas()
    pd.to_csv('Query13.csv', index=False)
    pd.set_index("keyword", drop=True, inplace=True)
    pd.plot.bar()
    plt.xticks(rotation=0, ha="right")
    plt.title("Inspect Based on Keywords")
    plt.ylabel('tweets count')
    # plt.axis('equal')
    plt.tight_layout()
    # plt.show()
    plt.savefig("graph13.png")
    graph13 = BytesIO()
    plt.savefig(graph13)
    graph13.seek(0)
    return graph13

# Query14
# Query to fetch top 10 countries
def get_query14():
    plt.clf()
    # os.remove("Query14.csv")
    # os.remove("graph14.png")
    query_14 = spark.sql(
        "select  place.country as country , count(*) as Count from tweets where place.country IS not null Group by place.country order by count DESC Limit 10")
    pd = query_14.toPandas()
    pd.to_csv('Query14.csv', index=False)
    x = pd.country.tolist()
    y = pd.Count.tolist()
    plt.stackplot(x, y)
    plt.title("Twitter Most Used Countries")
    plt.xlabel('Country')
    plt.ylabel('Count')
    plt.xticks(rotation=45, ha="right")
    # plt.axis('equal')
    plt.tight_layout()
    # plt.show()
    plt.savefig('graph14.png')
    graph14 = BytesIO()
    plt.savefig(graph14)
    graph14.seek(0)
    return graph14

# Query15
# Query to fetch top 10 followers based on location
def get_query15():
    plt.clf()
    # os.remove("Query15.csv")
    # os.remove("graph15.png")
    query_15 = spark.sql(
        "select screen_name as Name, location as Location, followers_count as FollowersCount  from user  where location is not null ORDER BY followers_count DESC LIMIT 10")
    pd = query_15.toPandas()
    pd.to_csv("Query15.csv", index=False)
    pd.plot.pie(y="FollowersCount", labels=pd.Name.tolist(), autopct='%1.2f%%', shadow=False, legend=False, fontsize=8)
    plt.title("Accounts With More Followers Count Based on Location")
    plt.axis('equal')
    plt.tight_layout()
    # plt.show()
    plt.savefig("graph15.png")
    graph15 = BytesIO()
    plt.savefig(graph15)
    graph15.seek(0)
    return graph15

# Query16
# Query to fetch retweeted tweets in english
def get_query16():
    plt.clf()
    # os.remove("Query16.csv")
    # os.remove("graph16.png")
    query_16 = spark.sql(
        "select text, count(*) as no_of_tweets from tweets where text is not null and retweeted_status.id is not null and lang='en' group by text order by no_of_tweets desc limit 10")
    pd = query_16.toPandas()
    pd.to_csv('Query16.csv', index=False)
    pd.plot.pie(y="no_of_tweets", labels=pd.text.tolist(), autopct='%1.2f%%', shadow=False, legend=False, fontsize=8)
    plt.title("Top Retweeted Tweets(Tweet in english language)")
    plt.axis('equal')
    plt.tight_layout()
    # plt.show()
    plt.savefig('graph16.png')
    graph16 = BytesIO()
    plt.savefig(graph16)
    graph16.seek(0)
    return graph16

# Query17
# Query to fetch number of tweets and users collected
def get_query17():
    plt.clf()
    # os.remove("Query17.csv")
    # os.remove("graph17.png")
    query_17 = spark.sql(
        "select(select count(*) from tweets)as tweets_count,(select count(*) from user)as Users_count")
    pd = query_17.toPandas()
    pd.to_csv('Query17.csv', index=False)
    plt.bar(x=pd.Users_count.tolist(), height=pd.tweets_count.tolist())
    plt.xlabel('Users count')
    plt.ylabel('tweets count')
    plt.title("Users & Tweets Analyzed for Processing")
    # plt.axis('equal')
    plt.tight_layout()
    # plt.show()
    plt.savefig('graph17.png')
    graph17 = BytesIO()
    plt.savefig(graph17)
    graph17.seek(0)
    return graph17