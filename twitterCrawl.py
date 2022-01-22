# tutorial.py
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import tweepy
# from tweepy.error import TweepError, RateLimitError
from pandas import pandas as pd
from airflow.operators.python_operator import PythonOperator

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("tubes") \
    .config("spark.mongodb.input.uri", "mongodb://tubes_bigdata:bigdata_tubes@cluster0-shard-00-00.uveba.mongodb.net:27017,cluster0-shard-00-01.uveba.mongodb.net:27017,cluster0-shard-00-02.uveba.mongodb.net:27017/big_data_hoax.tweet_data?ssl=true&replicaSet=atlas-1hf86r-shard-0&authSource=admin&retryWrites=true&w=majority") \
    .config("spark.mongodb.output.uri", "mongodb://tubes_bigdata:bigdata_tubes@cluster0-shard-00-00.uveba.mongodb.net:27017,cluster0-shard-00-01.uveba.mongodb.net:27017,cluster0-shard-00-02.uveba.mongodb.net:27017/big_data_hoax.tweet_data?ssl=true&replicaSet=atlas-1hf86r-shard-0&authSource=admin&retryWrites=true&w=majority") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1')\
    .getOrCreate()

dict_key = [{
    "consumer_key": "6ya2eSKmSv3jNVLgFwZZeDlkU",
    "consumer_sec": "pC2ssRJN2cn9Nbu6y3cZ6IE60eQaxAJmp0rD0rT2Dck601m6kP",
    "acc_token": "575905026-ALDwfyMhICZApr1uRZKeWRKxcubreAdqFpztZH3I",
    "acc_sec": "1mqj3KK3P0YSbeXoPUHZk4Kc9LkVDKSAnYlHCPrhLR60m",
},
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['email@mail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'Twitter_Flow_Crawling',
    default_args=default_args,
    description='A Flow data Twitter',
    schedule_interval=timedelta(days=1),
)


def insert_into_mongo(data):
    tweet_data = spark.createDataFrame(data)
    tweet_data.write.format(
        "com.mongodb.spark.sql.DefaultSource").mode("append").option("database", "big_data_hoax").option("collection", "tweet_data_gpresiden").save()


def get_auth():
    code_key = 0
    auth = tweepy.OAuthHandler(
        dict_key[code_key]["consumer_key"], dict_key[code_key]["consumer_sec"])
    auth.set_access_token(
        dict_key[code_key]["acc_token"], dict_key[code_key]["acc_sec"])
    api = tweepy.API(auth, wait_on_rate_limit=True)
    return api


def get_data_search(**kwargs):
    data = []
    query = "Tahun2022GantiPresiden"
    api = get_auth()
    cursor = tweepy.Cursor(api.search_tweets, result_type='mixed',
                           q=query, lang='id', include_entities=True).items()
    c = 0
    for x in cursor:
        c = c + 1
        print(x)
        data.append(x)
        if c == 2000:
            break
    return data


def parse_data(**context):
    all_data = []
    value = context['task_instance'].xcom_pull(task_ids='crawling_data')
    for tweet in value:
        print(tweet)
        dict_line = {
            "createdAt": str(tweet.created_at),
            "twitId": tweet.id,
            "twitContent": str(tweet.text.encode('ascii', 'ignore').decode("ascii")),
            "userId": tweet.user.id,
            # tweet.user.name.encode('ascii', 'ignore').decode("ascii"),
            "userName": tweet.user.screen_name,
            "location": tweet.user.location,
            "retweet": tweet.retweet_count,
            "like": tweet.favorite_count,
            "url": tweet.user.url,
            "listed_count": tweet.user.listed_count,
            "user_created_at": str(tweet.user.created_at),
            "followers_count": tweet.user.followers_count,
            "statuses_count": tweet.user.statuses_count,
        }
        all_data.append(dict_line)
    return all_data


def save_data(**context):
    value = context['task_instance'].xcom_pull(task_ids='parsing_data')
    insert_into_mongo(value)
    # Ad = pd.DataFrame(value)
    # print(Ad)
    # Ad.to_csv("marwan.csv")


t1 = PythonOperator(
    task_id='crawling_data',
    python_callable=get_data_search,
    # provide_context=True,
    dag=dag)

t2 = PythonOperator(
    task_id='parsing_data',
    python_callable=parse_data,
    provide_context=True,
    dag=dag)
t3 = PythonOperator(
    task_id='save_data',
    python_callable=save_data,
    provide_context=True,
    dag=dag)

t1 >> t2 >> t3
