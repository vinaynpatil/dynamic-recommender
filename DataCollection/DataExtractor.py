from TweetsListner import TweetsListner
import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from TweetProducer import TweetProducer
import collections
from collections import defaultdict
import json
import time

class DataExtractor:
    def __init__(self):
        self.genre_list = ['action', 'adventure','comedy','crime','animation','anime','biopic','childrens','detective','spy','documentary','drama','horror','family','fantasy','historical','medical','musical','paranormal','romance','sport','fiction','sci-fi','mystery','thriller','suspense','war','western']
        with open("../MovieData/actor1_handle.json",'r') as a1f:
          self.actor1_dict = json.load(a1f)
        with open("../MovieData/actor2_handle.json",'r') as a2f:
          self.actor2_dict = json.load(a2f)
        with open("../MovieData/director_handle.json",'r') as df:
          self.director_dict = json.load(df)
        with open("../MovieData/movie_handle.json",'r') as mf:
          self.movie_dict = json.load(mf)
        with open("../MovieData/movies_list_updated.json",'r') as m2f:
          self.movie_list = json.load(m2f)

    def create_tweet_listner(self):
        consumer_key ="pUVDi94pxaNgc214PQrhpPUwa"
        consumer_secret ="U8juTSRI2RazqQRIqlqp1qoWcaYVxH7bi2Ka3BKhdk5rPYcIVD"
        access_token ="994457489709125632-7jsHa4jzK2lN7ZoEi6Muv7aeNIWPDmp"
        access_token_secret ="FiJ0Budbxd3bvXi596vSK8Gc2ShmwoCeVTvtPDxLZTBo1"
        tweetListener = TweetsListner(consumer_key,consumer_secret,access_token,access_token_secret)
        #auth = OAuthHandler(consumer_key, consumer_secret)
        #auth.set_access_token(access_token, access_token_secret)
        userIds = ["994457489709125632","1048372701788868608","1048374435370164230"]
        #userIds = ["1048372701788868608"]
        api = tweepy.API(tweetListener.auth)
        self.createProfile(api,userIds)
        twitter_stream = Stream(tweetListener.auth, tweetListener,tweet_mode='extended')
        twitter_stream.filter(follow=userIds)

    def Upsert(self,noun,field,userDict):
            print ("invoked for",field,noun)
            if field in userDict:
                if noun in userDict[field]:
                    userDict[field][noun]['SP'] = 1
                    userDict[field][noun]['DP'] = 1
                    userDict[field][noun]['time'] = time.time()
                else:
                    userDict[field][noun] = {}
                    userDict[field][noun]['SP'] = 1
                    userDict[field][noun]['DP'] = 1
                    userDict[field][noun]['time'] = time.time()
            else:
                    userDict[field] = {}
                    userDict[field][noun] = {}
                    userDict[field][noun]['SP'] = 1
                    userDict[field][noun]['DP'] = 1
                    userDict[field][noun]['time'] = time.time()

    def UpdateSubCriteria(self,userDict,movie_name):
        print ("UpdateSubCriteria")
        for item in self.movie_list:
            if item['movie_handle'] == movie_name:
                if item['actor_1_handle']:
                    self.Upsert(item['actor_1_handle'],'actor',userDict)
                if item['actor_2_handle']:
                    self.Upsert(item['actor_2_handle'],'actor',userDict)
                if item['director_handle']:
                    self.Upsert(item['director_handle'],'director',userDict)
                if len(item['genre']) > 0:
                    for gen in item['genre']:
                        print (gen)
                        self.Upsert(gen,'genre',userDict)

    def createProfile(self,api,userIds):
        self.genre_list = ['action', 'adventure','comedy','crime','animation','anime','biopic','childrens','detective','spy','documentary','drama','horror','family','fantasy','historical','medical','musical','paranormal','romance','sport','fiction','sci-fi','mystery','thriller','suspense','war','western']
        profile = collections.defaultdict(list)
        for user in userIds:
            userDict = defaultdict(dict)
            userDetails = api.get_user(user)
            f =open("../ProfileData/"+str(user)+".json","w")
            friends = []
            for friend in api.friends_ids(user):
                noun = api.get_user(friend).screen_name.lower()
                print ("following",noun)
                found = False
                try:
                  if self.actor1_dict[noun]:
                    self.Upsert(noun,'actor',userDict)
                    found = True
                except:
                  pass
                if not(found):
                  try:
                    if self.actor2_dict[noun]:
                      self.Upsert(noun,'actor',userDict)
                      found = True
                  except:
                    pass
                if not(found):
                  try:
                    if self.director_dict[noun]:
                      self.Upsert(noun,'director',userDict)
                      found = True
                  except:
                    pass
                if not(found):
                  try:
                    #print ("invoked for movie",noun)
                    if self.movie_dict[noun]:
                      #self.Upsert(noun,'movie',userDict)
                      #need to add subcriteria
                      print ("invoked for movie",noun)
                      self.UpdateSubCriteria(userDict,noun)
                      found = True
                  except:
                    pass
                if not(found) and (friend in self.genre_list):
                    self.Upsert(noun,'genre',userDict)
                    found = True


            f.write(json.dumps(userDict))
            f.close()
            
            profile[userDetails.screen_name] =  {
                'id' : user,
                'location' : userDetails.location,
                'friends_count' : userDetails.friends_count,
                'friends_list' : friends
            }
            
        print(profile)

@classmethod
def parse(cls, api, raw):
    print ("parse called")
    status = cls.first_parse(api, raw)
    setattr(status, 'json', json.dumps(raw))
    return status
    

   
if __name__ == '__main__':
    DataExtractor().create_tweet_listner()