import praw
import pandas as pd
import datetime as datetime
from pymongo import MongoClient

#Set up MongoDB connection
MONGO_HOST= 'mongodb://127.0.0.1:27017/webScience'
client = MongoClient(MONGO_HOST)
db = client.redditdb

#Authentication for reddit API
reddit = praw.Reddit(client_id='client_id', \
                     client_secret='client_secret', \
                     user_agent='web science reddit crawler', \
                     username='username', \
                     password='password')

#Search for the sub-reddit r/Singapore
subreddit = reddit.subreddit('singapore')

#Find the top submissions in r/Singapore within the month
for submission in subreddit.top('month', limit=None):

	submission.comments.replace_more(limit=None)

	#Iterate through all the comments in the top submissions
	for comment in submission.comments.list():
		
		data={}
		data['time'] = (comment.created_utc)
		data['author'] = ("%s") % comment.author
		data['text'] = comment.body
		data['score'] = comment.score
		data['type'] = "comments"
		db.redditdb.insert(data)

	data={}
	data['time'] = (submission.created_utc)
	data['author'] = ("%s") % submission.author
	data['text'] = submission.title
	data['score'] = submission.score
	data['type'] = "submission"
	db.redditdb.insert(data)

print(datetime.datetime.now())
print(counter)