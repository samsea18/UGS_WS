import threading
import tweepy
import json
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import time
import re

#Set up Twitter API authentication and access token
CONSUMER_KEY = "CONSUMER_KEY"
CONSUMER_SECRET = "CONSUMER_SECRET"
ACCESS_TOKEN = "ACCESS_TOKEN"
ACCESS_TOKEN_SECRET = "ACCESS_TOKEN_SECRET"

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

#Set up the listener. The 'wait_on_rate_limit=True' is needed to cope with Twitter API rate limiting.
api = tweepy.API(auth, wait_on_rate_limit = True)

#Establish database connection
MONGO_HOST= 'mongodb://127.0.0.1:27017/webScience'
client = MongoClient(MONGO_HOST) 
# Use webScience database. If it doesn't exist, it will be created.
db = client.webScience


#--------------------------------------------------------------------Pre-filtering of trending topics in SG
trends = api.trends_place(23424948)
# or 1062617

sgTrends = [trend['name'] for trend in trends[0]['trends']]

trendname = ''
if (len(trends) > 40) :
	for x in range(len(trends) -10):
	    if x == 0:
	        trendname = '"' + trends[x] + '"'
	    else:
	        trendname = trendname + 'OR' + '"' + trends[x] + '"' + "since:2018-11-17"

else:
	for x in range(len(trends) -1):
	    if x == 0:
	        trendname = '"' + trends[x] + '"'
	    else:
	        trendname = trendname + 'OR' + '"' + trends[x] + '"' + "since:2018-11-17"


trendingGroups = [x.lower() for x in [re.sub(r'\W', '', i) for i in sgTrends]]

#--------------------------------------------------------------------End of pre-filtering methods


#------------------------------------------------------------------------------- Start of Streaming method
class StreamListener(tweepy.StreamListener):    
	#This is a class provided by tweepy to access the Twitter Streaming API. 
	def __init__(self, time_limit):
		self.start_time = time.time()
		self.limit = time_limit
		super(StreamListener, self).__init__()

	def on_connect(self):
		#When connected to the Streaming API
		print("---------------------------------------------------------------")
		print("You are now connected to the streaming API. Starting stream... ")
		print("---------------------------------------------------------------\n")
 
	def on_error(self, status_code):
		# If an error occurs, display the error / status code
		print('An Error has occured: ' + repr(status_code))
		return False

	def on_data(self, data):

		try:
			#Run streaming for 1 hour
			if (time.time() - self.start_time) < 60 * 60:

                #Decode the JSON object from Twitter
				datajson = json.loads(data)
				text = datajson['text']

				#Check for truncated tweets
				if datajson['truncated'] is True:

					if datajson['place'] is not None :

						if (datajson['place']['country_code'] == 'SG') :
							created_at = datajson['created_at']
							print(text + created_at + "            via streamer")

							trendtype = groupTrends(datajson['extended_tweet']['full_text'])

							collectedStream = {"collected_time" :datetime.now(), "API" :"STREAMING", "Trend_group":trendtype}
							collectedStream.update(datajson)
							db.twitterCol.insert(collectedStream)

						else :
							pass

					else :

						#grab the 'created_at' data from the Tweet to use for display
						created_at = datajson['created_at']
						text = datajson['text']

						#print out a message to the screen that we have collected a tweet
						print(text + created_at + "            via streamer")

						trendtype = groupTrends(datajson['extended_tweet']['full_text'])

						collectedStream = {"API" :"STREAMING", "Trend_group":trendtype}
						collectedStream.update(datajson)
						db.twitterCol.insert(collectedStream)


				else:

					if datajson['place'] is not None :

						if (datajson['place']['country_code'] == 'SG') :
							created_at = datajson['created_at']
							print(text + created_at + "            via streamer")

							trendtype = groupTrends(datajson['text'])

							collectedStream = {"collected_time" :datetime.now(), "API" :"STREAMING", "Trend_group":trendtype}
							collectedStream.update(datajson)
							db.twitterCol.insert(collectedStream)

						else :
							pass

					else :

						#grab the 'created_at' data from the Tweet to use for display
						created_at = datajson['created_at']
						text = datajson['text']

						#print out a message to the screen that we have collected a tweet
						print(text + created_at + "            via streamer")

						trendtype = groupTrends(datajson['text'])

						collectedStream = {"API" :"STREAMING", "Trend_group":trendtype}
						collectedStream.update(datajson)
						db.twitterCol.insert(collectedStream)

			else:
				print("---------------------------")
				print("     END OF STREAMING      ")
				print("---------------------------")
				return False

		except Exception as e:
			print(e)
 
def streamer(api, auth):
    #global api, auth   
	streamer = tweepy.Stream(auth=auth, listener=StreamListener(api), tweet_mode='extended', async=True)

	#south,west,north,east        co-ord from https://www.flickr.com/places/info/23424948
	streamer.filter(locations=[103.618248,1.1158,104.40847, 1.47062], languages=["en"])

#------------------------------------------------------------------------------- End of Streaming method    


#------------------------------------------------------------------------------- Start of REST API method
def restapi(api):
	global sgTrends

	print("---------------------------")
	print("     STARTING REST API     ")
	print("---------------------------")
	time.sleep(5)

	location = "1.360274330941498,103.81241651748661,25km"

	cursor = tweepy.Cursor(api.search, q=trendname, count=100, geocode=location, include_entities=True, lang='en', tweet_mode='extended')

	for tweet in(cursor.items()):
	
			datajson = tweet._json

			try:
				print(tweet.full_text + str(tweet.created_at) + "            full text via REST API")

				trendtype = groupTrends(datajson['full_text'])

				print(trendtype)
				collectedREST = {"API" :"REST", "Trend_group":trendtype}
				
				#Replace text with full text
				datajson['text'] = datajson.pop('full_text')
				
				collectedREST.update(datajson)
				db.twitterCol.insert(collectedREST)

			except:
				print(tweet.text + str(tweet.created_at) + "            via REST API")

				trendtype = groupTrends(datajson['text'])

				print(trendtype)
				collectedREST = {"API" :"REST", "Trend_group":trendtype}
				collectedREST.update(datajson)
				db.twitterCol.insert(collectedREST)

#------------------------------------------------------------------------------- End of REST API method


#------------------------------------------------------------------------------- Start of Trend filter method
def groupTrends(tweet):
	if any(d in tweet.lower() for d in trendingGroups):
		found = [t for t in trendingGroups if t in tweet.lower()]
		trendtype = found[0]
	else:
		trendtype = "Others"

	return trendtype

#------------------------------------------------------------------------------- End of Trend filter method


# Create new threads
thread1 = threading.Thread(target=streamer,args=(api, auth))
thread2 = threading.Thread(target=restapi,args=(api,))

# Display Trending
print("")
print(pd.DataFrame(sgTrends))
print("")

# Start new Threads
thread1.start()
time.sleep( 3610 )
thread2.start()
