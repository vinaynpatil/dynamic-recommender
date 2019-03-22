import json
import requests
from nltk.tokenize.punkt import PunktSentenceTokenizer, PunktTrainer
import spacy
import urllib3
from pprint import pprint
import time
import random
import RecoList
from kafkaProducer import Producer
from kafka import KafkaConsumer



class Simulator:
  def __init__(self):
    print("Constructed")
    self.userid = 1048372701788868608

  def consumerMessage(self):
      print("Called consumer")
      consumer = KafkaConsumer('triggersimulator',bootstrap_servers=['localhost:9092'])
      for message in consumer:

        r_msg = str(message.value.decode("utf-8"))
        reco_obj = RecoList.RecList()
        self.simualteResults(reco_obj.computeReco(self.userid),reco_obj,self.userid)
        
  def simualteResults(self,recoList,reco_obj,userId):
      movie_watch_list = []
      no_of_movies = len(recoList)
      movies_watched = random.randint(1,no_of_movies)
      for i in range(movies_watched):
        movie_no = random.randint(1,no_of_movies)
        while (movie_no in movie_watch_list):
          movie_no = random.randint(1,no_of_movies)
        movie_watch_list.append(movie_no)
        
      criteria_list = ["movie_handle","actor_1_handle","director_handle","genre"]
      role_list = ["movie","actor","director","genre"]
      watched_list = []
      final_msg = []
      output_data ={}
      msg_dict ={}

      for movie in movie_watch_list:
       progress = random.randint(0,100)
       watched_list.append(recoList[movie-1][0]['movie_handle'])

       polarity = (progress-50)*2/100
       criteria = random.randint(0,3)
       selected_movie = recoList[movie-1][0]
       if criteria == 3:
        selected_handle = selected_movie[criteria_list[criteria]][0]
       else:
        selected_handle = selected_movie[criteria_list[criteria]]
       msg_dict['role'] = role_list[criteria]
       msg_dict['value'] = selected_handle
       msg_dict['sentiment'] = polarity
       final_msg.append(msg_dict)
      output_data['type'] = 'simulator'
      output_data['msg'] = final_msg
      output_data['userid'] = userId

      Producer().produceMessage(json.loads(json.dumps(output_data)),"sentiments")

      user_data = reco_obj.user_data

      if 'watched_list' in user_data:
        user_data['watched_list'].extend(watched_list)
      else:
        user_data['watched_list'] = watched_list


      with open("ProfileData/"+str(userId)+".json","w") as f:
        user_data = json.dumps(user_data)
        f.write(user_data)


simulator = Simulator()
reco_obj = RecoList.RecList()
simulator.simualteResults(reco_obj.computeReco(simulator.userid),reco_obj,simulator.userid)
simulator.consumerMessage()
