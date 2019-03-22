from textblob import TextBlob
import json
from nltk import sent_tokenize
from textblob.taggers import NLTKTagger
import requests
from nltk.tokenize.punkt import PunktSentenceTokenizer, PunktTrainer
import spacy
import urllib3
from pprint import pprint
import time


class RecList:
  def computeReco(self,userId):
      print ("compute Reco called")
      with open("../MovieData/movies_list_updated.json",'r') as m2f:
        #data = mf.read()
        movie_list = json.load(m2f)
      with open("ProfileData/"+str(userId)+".json") as f:
        self.user_data = json.load(f)
      movie_dict = list()
      watched_list = []
      if 'watched_list' in self.user_data:
        watched_list = self.user_data['watched_list']

      for movie in movie_list:
        score = 0

        if movie['movie_handle'] not in watched_list:
          for key,val in self.user_data.items():
            if key !='watched_list':
              for key1,val1 in val.items():
                if key1 == movie['actor_1_handle'] or key1 == movie['actor_2_handle']:
                  score += val1['DP']
                elif key1 == movie['director_handle']:
                  score += val1['DP']
                else:
                  for gen in movie['genre']:
                    if key1 == gen:
                      score += val1['DP']
          movie_dict.append([movie,score])

      final_list = sorted(movie_dict,key = lambda x: x[1],reverse=True)
      print (final_list[0:10])
      return final_list[0:10]

#RecList().computeReco(1048372701788868608)