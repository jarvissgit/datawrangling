#WHO TWEETED MOST?
#-group tweets by user
#-count each user's tweet
#-sort into descending order
#-select user at top

from pymongo import MongoClient
import pprint

client = MongoClient("mongodb://localhost:27017")
db = client.twitter

def most_tweets():
    result = db.tweets.aggregate([
        { "$group" : 


