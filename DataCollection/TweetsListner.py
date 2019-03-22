import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import requests
import sys
sys.path.append("..")
from kafkaProducer import Producer

class TweetsListner(StreamListener):

	def __init__(self,consumer_key,consumer_secret,access_token,access_token_secret):
		self.auth = OAuthHandler(consumer_key, consumer_secret)
		self.auth.set_access_token(access_token, access_token_secret)
 
	def on_data(self, data):
		try:
			self.tweet_text_data = json.loads(data)
			if 'extended_tweet' in self.tweet_text_data :
				print( "extended_tweet")
				self.tweet_text = self.tweet_text_data['extended_tweet']['full_text']
			else:
				self.tweet_text = self.tweet_text_data.get("text")
				print( "text")
			self.tweet_id = self.tweet_text_data.get("id_str")
			print (self.tweet_text)
			print (self.tweet_id)
			self.json_data = {"userid":self.tweet_id,"tweet_data":self.tweet_text_data,"source":"twitter","type":""}
			#self.json_data = json.dumps(tweet_data)
			print (json.dumps(self.json_data))
			Producer().produceMessage(self.json_data,"test")
			return True
		except BaseException as e:
		    print("Error on_data: %s" % str(e))
		return True
 
	def on_error(self, status):
		print(status)
		return True

@classmethod
def parse(cls, api, raw):
	status = cls.first_parse(api, raw)
	setattr(status, 'json', json.dumps(raw))
	return status