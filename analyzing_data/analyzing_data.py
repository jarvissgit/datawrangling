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
        { "$group" : { "_id" : "$user.screen_name",
                      "count" : { "$sum" : 1 } } },
        { "$sort" : { "count" : -1 } } ])
    return result
# $user.screen_name means to group all user.screen_name entries based on the value of user.screen_name
# $sum is the accumulator operator
# the first line after $group says that for each same value of user.screen_name, increment the value of count by 1

#refer aggregation_pipeline.png

if __name__ == '__main__':
    result = most_tweets()
    pprint.pprint(result)

