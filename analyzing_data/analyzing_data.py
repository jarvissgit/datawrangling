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
# the group stage changes the document format as specified to do its job
# it is not necessary to do group in the first stage and sort in the last stage. The type of aggregations and their order is entirely dependent on the application

#refer aggregation_pipeline.png

if __name__ == '__main__':
    result = most_tweets()
    pprint.pprint(result)

# Quiz: Using Group

#!/usr/bin/env python
"""
The tweets in our twitter collection have a field called "source". This field describes the application
that was used to create the tweet. Following the examples for using the $group operator, your task is 
to modify the 'make-pipeline' function to identify most used applications for creating tweets. 
As a check on your query, 'web' is listed as the most frequently used application.
'Ubertwitter' is the second most used. The number of counts should be stored in a field named 'count'
(see the assertion at the end of the script).

Please modify only the 'make_pipeline' function so that it creates and returns an aggregation pipeline
that can be passed to the MongoDB aggregate function. As in our examples in this lesson, the aggregation 
pipeline should be a list of one or more dictionary objects. 
Please review the lesson examples if you are unsure of the syntax.

Your code will be run against a MongoDB instance that we have provided. 
If you want to run this code locally on your machine, you have to install MongoDB, 
download and insert the dataset.
For instructions related to MongoDB setup and datasets please see Course Materials.

Please note that the dataset you are using here is a smaller version of the twitter dataset 
used in examples in this lesson. 
If you attempt some of the same queries that we looked at in the lesson examples,
your results will be different.
"""


def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def make_pipeline():
    # complete the aggregation pipeline
    pipeline = ([{"$group" : {"_id" : "$source",
                    "count" : {"$sum" : 1}}},{"$sort": {"count" : -1}}])
    return pipeline

def tweet_sources(db, pipeline):
    return [doc for doc in db.tweets.aggregate(pipeline)]

if __name__ == '__main__':
    db = get_db('twitter')
    pipeline = make_pipeline()
    result = tweet_sources(db, pipeline)
    import pprint
    pprint.pprint(result[0])
    assert result[0] == {u'count': 868, u'_id': u'web'}

#---------------------------------------------------
# AGGREGATION OPERATORS 1
#---------------------------------------------------

## $project OPERATOR
#$project can take the documents and perform a number of operations. Selection of fields is just one of them!

## $match OPERATOR

## $group OPERATOR

## $sort OPERATOR

## $skip OPERATOR
# $skip allows us to skip over a certain number of documents from our input set of documents

## $limit OPERATOR
# $limit operator limits the number of documents sent to the next stage

#---------------------------------------------------
# AGGREGATION OPERATORS 2
#---------------------------------------------------

# $unwind operator
#$unwind operator will unwind the array values of a field in input document to individual documents with the value in its field
#refer unwind.png


#---------------------------------------------------
# MATCH OPERATOR
#---------------------------------------------------

#WHO HAS THE HIGHEST FOLLOWERS TO FRIENDS RATIO ?

#It is important to note that there is no "$group" stage in this pipeline - if a user tweeted more than once in this data set, a ratio will be computed for that user each time one of their tweets is found, leading to duplicates. If the user's friend count or followers count changed during the time this data set was taken, the ratio will change too!

def highest_ratio():
    result = db.tweets.aggregate([
        { "$match" : { "user.friends_count": { "$gt" : 0 },
                      "user.follower_count": { "$gt" : 0 } } },
        { "$project" : { "ratio" : { "$divide" : ["$user.followers_count",
                                                  "$user.friends_count"]},
                        "screen_name" : "$user.screen_name"} },
        { "$sort" : { "ratio" : -1 } },
        { "$limit" : 1 } ])
    return result

if __name__ == '__main__':
    result = highest_ratio()

#we use the same syntax for match as the read or find operations

"""
Use $project to 

-include fields from the original document[project works with a single document at a time]
-insert computed fields 
-rename fields
-create fields that hold subdocuments
"""
#-------------------------------------------------------
#QUIZ : USING MATCH AND PROJECT OPERATOR
#-------------------------------------------------------
#!/usr/bin/env python
"""
Write an aggregation query to answer this question:

Of the users in the "Brasilia" timezone who have tweeted 100 times or more,
who has the largest number of followers?

The following hints will help you solve this problem:
- Time zone is found in the "time_zone" field of the user object in each tweet.
- The number of tweets for each user is found in the "statuses_count" field.
  To access these fields you will need to use dot notation (from Lesson 4)
- Your aggregation query should return something like the following:
{u'ok': 1.0,
 u'result': [{u'_id': ObjectId('52fd2490bac3fa1975477702'),
                  u'followers': 2597,
                  u'screen_name': u'marbles',
                  u'tweets': 12334}]}
Note that you will need to create the fields 'followers', 'screen_name' and 'tweets'.

Please modify only the 'make_pipeline' function so that it creates and returns an aggregation 
pipeline that can be passed to the MongoDB aggregate function. As in our examples in this lesson,
the aggregation pipeline should be a list of one or more dictionary objects. 
Please review the lesson examples if you are unsure of the syntax.

Your code will be run against a MongoDB instance that we have provided. If you want to run this code
locally on your machine, you have to install MongoDB, download and insert the dataset.
For instructions related to MongoDB setup and datasets please see Course Materials.

Please note that the dataset you are using here is a smaller version of the twitter dataset used 
in examples in this lesson. If you attempt some of the same queries that we looked at in the lesson 
examples, your results will be different.
"""

def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def make_pipeline():
    # complete the aggregation pipeline
    pipeline = [{"$match" : {"user.time_zone" : "Brasilia",
              "user.statuses_count": {"$gte": 100 }}},
{"$project" : {"_id":"$_id",
              "followers":"$user.friends_count",
              "screen_name" : "$user.screen_name",
              "tweets" : "$user.statuses_count"} },
{"$sort":{"followers":-1} },
{"$limit":1}]
    return pipeline

def aggregate(db, pipeline):
    return [doc for doc in db.tweets.aggregate(pipeline)]


if __name__ == '__main__':
    db = get_db('twitter')
    pipeline = make_pipeline()
    result = aggregate(db, pipeline)
    import pprint
    pprint.pprint(result)
    assert len(result) == 1
    assert result[0]["followers"] == 17209
"""
NOTE: In the current version of pymongo (3.0), aggregation operations return a cursor object. In order to see the elements returned, you can iterate over the cursor object, such as using a for loop, and print out the elements one by one. Keep this in mind when working on your local machine.

Example Tweet:

{
    "_id" : ObjectId("5304e2e3cc9e684aa98bef97"),
    "text" : "First week of school is over :P",
    "in_reply_to_status_id" : null,
    "retweet_count" : null,
    "contributors" : null,
    "created_at" : "Thu Sep 02 18:11:25 +0000 2010",
    "geo" : null,
    "source" : "web",
    "coordinates" : null,
    "in_reply_to_screen_name" : null,
    "truncated" : false,
    "entities" : {
        "user_mentions" : [ ],
        "urls" : [ ],
        "hashtags" : [ ]
    },
    "retweeted" : false,
    "place" : null,
    "user" : {
        "friends_count" : 145,
        "profile_sidebar_fill_color" : "E5507E",
        "location" : "Ireland :)",
        "verified" : false,
        "follow_request_sent" : null,
        "favourites_count" : 1,
        "profile_sidebar_border_color" : "CC3366",
        "profile_image_url" : "http://a1.twimg.com/profile_images/1107778717/phpkHoxzmAM_normal.jpg",
        "geo_enabled" : false,
        "created_at" : "Sun May 03 19:51:04 +0000 2009",
        "description" : "",
        "time_zone" : null,
        "url" : null,
        "screen_name" : "Catherinemull",
        "notifications" : null,
        "profile_background_color" : "FF6699",
        "listed_count" : 77,
        "lang" : "en",
        "profile_background_image_url" : "http://a3.twimg.com/profile_background_images/138228501/149174881-8cd806890274b828ed56598091c84e71_4c6fd4d8-full.jpg",
        "statuses_count" : 2475,
        "following" : null,
        "profile_text_color" : "362720",
        "protected" : false,
        "show_all_inline_media" : false,
        "profile_background_tile" : true,
        "name" : "Catherine Mullane",
        "contributors_enabled" : false,
        "profile_link_color" : "B40B43",
        "followers_count" : 169,
        "id" : 37486277,
        "profile_use_background_image" : true,
        "utc_offset" : null
    },
    "favorited" : false,
    "in_reply_to_user_id" : null,
    "id" : NumberLong("22819398300")
}

"""

#-------------------------------------------------------
#UNWIND OPERATOR
#-------------------------------------------------------

# WHO INCLUDED THE MOST USER MENTIONS?

"""
mongo shell commands

db.tweets.find({"entities.user_mentions" : {"$size" : 3} } )
"""

def user_mentions():
    result = dbtweetsaggregate([
        {"$unwind" : "$entities.user_mentions"},
        {"$group" : { "_id" : "$user.screen_name",
                     "count" : { "$sum" : 1 } } },
        { "$sort" : { "count" : -1 } },
        { "$limit" : 1 } ] )

# unwind will create a duplicate of the document except for the array field where there will be only one user mention per document

#---------------------------------
# QUIZ : UNWIND
#---------------------------------

#!/usr/bin/env python
"""
For this exercise, let's return to our cities infobox dataset. The question we would like you to answer
is as follows:  Which region or district in India contains the most cities? (Make sure that the count of
cities is stored in a field named 'count'; see the assertions at the end of the script.)

As a starting point, use the solution for the example question we looked at -- "Who includes the most
user mentions in their tweets?"

One thing to note about the cities data is that the "isPartOf" field contains an array of regions or 
districts in which a given city is found. See the example document in Instructor Comments below.

Please modify only the 'make_pipeline' function so that it creates and returns an aggregation pipeline 
that can be passed to the MongoDB aggregate function. As in our examples in this lesson, the aggregation 
pipeline should be a list of one or more dictionary objects. Please review the lesson examples if you 
are unsure of the syntax.

Your code will be run against a MongoDB instance that we have provided. If you want to run this code 
locally on your machine, you have to install MongoDB, download and insert the dataset.
For instructions related to MongoDB setup and datasets please see Course Materials.

Please note that the dataset you are using here is a smaller version of the cities collection used in 
examples in this lesson. If you attempt some of the same queries that we looked at in the lesson 
examples, your results may be different.
"""

def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def make_pipeline():
    # complete the aggregation pipeline
    pipeline = [{"$match" : {"country" : "India"}},
                {"$unwind" : "$isPartOf"},
                {"$group" : {"_id" : "$isPartOf",
                            "count" : {"$sum" : 1}}},
                {"$sort" : {"count" : -1}},
                {"$limit" : 1}]
    return pipeline

def aggregate(db, pipeline):
    return [doc for doc in db.cities.aggregate(pipeline)]

if __name__ == '__main__':
    db = get_db('examples')
    pipeline = make_pipeline()
    result = aggregate(db, pipeline)
    print "Printing the first result:"
    import pprint
    pprint.pprint(result[0])
    assert result[0]["_id"] == "Uttar Pradesh"
    assert result[0]["count"] == 623

"""

NOTE: In the current version of pymongo (3.0), aggregation operations return a cursor object. In order to see the elements returned, you can iterate over the cursor object, such as using a for loop, and print out the elements one by one. Keep this in mind when working on your local machine.

This is an example document:

{
    "_id" : ObjectId("52fe1d364b5ab856eea75ebc"),
    "elevation" : 1855,
    "name" : "Kud",
    "country" : "India",
    "lon" : 75.28,
    "lat" : 33.08,
    "isPartOf" : [
        "Jammu and Kashmir",
        "Udhampur district"
    ],
    "timeZone" : [
        "Indian Standard Time"
    ],
    "population" : 1140
}
Tip: Don't forget that we are concerned only with regions in India when you are constructing your pipeline.

"""
# THIS CODE WORKS WELL!!!


#---------------------------------
# GROUP ACCUMULATION OPERATORS
#---------------------------------

# $sum

# $first

# $last

# $max

# $min

# $avg
def hashtag_retweet_avg():
    result = db.tweets.aggregate([
        { "$unwind" : { "id" : "$entities.hashtags.text",
                       "retweet_avg" : {"$avg" : "$retweet_count"}
                      } },
        { "$sort" : { "retweet_avg" : -1 } } ] )
    
    return result

if __name__ == '__main__':
    result = hashtag_retweet_avg()
    pprint.pprint(result)

# $push [deals with arrays]


# $addToSet [deals with arrays, adds to the array like a set i.e. does not add duplicate values, ]
def unique_hashtags_by_user():
    result = db.tweets.aggregate([
        { "$unwind" : "$entities.hashtags"},
        { "$group" : { "_id" : "$user.screen_name",
                      "unique_hashtags" : {
                          "$addToSet" : "$entities.hashtags.text"
                      } } },
        { "$sort" : { "_id" : -1 } } ] )
    
    return result

if __name__ == '__main__':
    result = unique_hashtags_by_user()

#-----------------------------------------------------------
#QUIZ : using push
#----------------------------------------------------------

#!/usr/bin/env python
"""
$push is similar to $addToSet. The difference is that rather than accumulating only unique values 
it aggregates all values into an array.

Using an aggregation query, count the number of tweets for each user. In the same $group stage, 
use $push to accumulate all the tweet texts for each user. Limit your output to the 5 users
with the most tweets. 
Your result documents should include only the fields:
"_id" (screen name of user), 
"count" (number of tweets found for the user),
"tweet_texts" (a list of the tweet texts found for the user).  

Please modify only the 'make_pipeline' function so that it creates and returns an aggregation 
pipeline that can be passed to the MongoDB aggregate function. As in our examples in this lesson, 
the aggregation pipeline should be a list of one or more dictionary objects. 
Please review the lesson examples if you are unsure of the syntax.

Your code will be run against a MongoDB instance that we have provided. If you want to run this code 
locally on your machine, you have to install MongoDB, download and insert the dataset.
For instructions related to MongoDB setup and datasets please see Course Materials.

Please note that the dataset you are using here is a smaller version of the twitter dataset used in 
examples in this lesson. If you attempt some of the same queries that we looked at in the lesson 
examples, your results will be different.
"""

def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def make_pipeline():
    # complete the aggregation pipeline
    pipeline = [{"$group" : { "_id" : "$user.screen_name",
                            "count" : {"$sum" : 1},
                            "tweet_texts" : {"$push": "$text"}}},
                {"$sort" : {"$count": -1}},
                {"$limit":5}]
    return pipeline

def aggregate(db, pipeline):
    return [doc for doc in db.twitter.aggregate(pipeline)]


if __name__ == '__main__':
    db = get_db('twitter')
    pipeline = make_pipeline()
    result = aggregate(db, pipeline)
    import pprint
    pprint.pprint(result)
    assert len(result) == 5
    assert result[0]["count"] > result[4]["count"]
    sample_tweet_text = u'Take my money! #liesguystell http://movie.sras2.ayorganes.com'
    assert result[4]["tweet_texts"][0] == sample_tweet_text
   
#some error in the pipeline 
