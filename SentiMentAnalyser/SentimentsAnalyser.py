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
import sys
sys.path.append("..")
from kafkaProducer import Producer

class sentimentsAnalyser:
  '''
  ""TODO"" : 1. Tokanize the sentence based on context (like but,and) or based on punctuation (",","." etc)
             2.  Extracting a noun  from the sentence
             3. Associate a sentmentints to known 
             4. Generate a Dynamic profile

  '''
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

  def punCtuateSentence(self,msg):
    http = urllib3.PoolManager()
    payload = "text="+ msg
    data = {'text': msg}
    resp = http.request(
    "POST", "http://bark.phon.ioc.ee/punctuator", 
    body=payload)
    print (resp.content)

  '''
  add all conjunction to the sentence boundries 
  '''
  def set_custom_boundaries(self,doc):
    for token in doc[:-1]:
      if token.text in ('but' , 'yet' , 'and','however','or','though','although','eventhough','while' ):
        doc[token.i+1].is_sent_start = True
    return doc


  def spacySentenceSeggrigator(self,msg):
    nlp = spacy.load('en_core_web_sm')
    nlp.add_pipe(self.set_custom_boundaries, before='parser')
    doc = nlp(msg)
    print('seggrgated sentences:', [sent.text for sent in doc.sents])
    return  [sent.text for sent in doc.sents]

  '''
  returns all the nouns in the original text and screen_name corresponding 
  to the original tweeter so that sentiments can be assocated to them

  if retweet us empty or postive (single sentece) without noun
    assign polarity of original tweet

  else if negtive tweet without tags
    assign opposit polarity of original tweet
  else if :
    take polarity from retweet and those tags not in rtweet take tags original polarity.
  else:
    matching tags compare polariry and take poliryt of users if tags not in 
  '''

  def computeNounTagsFromrtweetRelation(self,retweet_text,msg,noun_sentiment_dict):
      original_tweet = msg['tweet_data']['quoted_status']['text']
    
      # correct or add punctuation marks to the sentence so that splitting is correct.
      correctedMsg = TextBlob(original_tweet).correct()
      punctuated_msg = self.punCtuateSentence(str(correctedMsg))
      punctuated_msg = str(correctedMsg)
      original_items =[]
      retweet_items =[]
      original_items = self.extractAndAssignPolarity(original_tweet.lower())
      if len(retweet_text) == 0:
        return original_items
      else:
        retweet_items = self.extractAndAssignPolarity(retweet_text.lower())
        if len(retweet_items) == 0:
          analysed_sent = TextBlob(retweet_text.lower())
          sentiment = analysed_sent.sentiment.polarity
          if(sentiment<0):
            for orig in original_items:
              orig['sentiment'] = orig['sentiment']*(-1)
          
        else:
          for orig in original_items:
            for retweet in retweet_items:
              if orig['value'] == retweet['value']:
                if (orig['sentiment'] > 0 and retweet['sentiment'] < 0) or (orig['sentiment'] < 0 and retweet['sentiment'] > 0 ):
                  orig['sentiment'] = retweet['sentiment']

        print(original_items)
        return original_items


      if len(original_tweet) > 1:
        split_words = original_tweet.split()
        for word in split_words:
          if word.startswith("@") or (word in self.genre_list):
            nouns_list.append(msg['quoted_status']['user']['screen_name'])
        if msg['quoted_status']['user']['screen_name']:
           print ("getNounsFromRetweet appending",msg['quoted_status']['user']['screen_name'])
           nouns_list.append(msg['quoted_status']['user']['screen_name'])
        print ("nouns_list",nouns_list)
        return nouns_list


  def getNounsFromRetweet(self,msg):
    nouns_list =[]
    if 'quoted_status' in msg:
      original_tweet = msg['quoted_status']['text']
      split_words = original_tweet.split()
      for word in split_words:
        if word.startswith("@") or (word in self.genre_list):
          nouns_list.append(msg['quoted_status']['user']['screen_name'])
      if msg['quoted_status']['user']['screen_name']:
         print ("getNounsFromRetweet appending",msg['quoted_status']['user']['screen_name'])
         nouns_list.append(msg['quoted_status']['user']['screen_name'])
    print ("nouns_list",nouns_list)
    return nouns_list

  def extractAndAssignPolarity(self,text):
      seggrgated_sentence_list = self.spacySentenceSeggrigator(text)

      #extracting a nouns 
      nltk_tagger = NLTKTagger()

      #use below code which will not analyse everytime and hence makes it faster.
      criteria_list = []
      for sentence in seggrgated_sentence_list:
        #invoke sentiment analyser
        analysed_sent = TextBlob(sentence)
        sentiment = analysed_sent.sentiment
        print (sentiment)
        # logic to idenityfy character starting with @ and considering them as noun
        split_words = sentence.split()
        for word in split_words:
          found = False
          noun_sentiment_dict ={}
          if word.startswith("@"):
            noun= word[1:]
            try:
              if self.actor1_dict[noun]:
                noun_sentiment_dict['role'] = 'actor'
                found = True
            except:
              pass
            if not(found):
              try:
                if self.actor2_dict[noun]:
                  noun_sentiment_dict['role'] = 'actor'
                  found = True
              except:
                pass
            if not(found):
              try:
                if self.director_dict[noun]:
                  noun_sentiment_dict['role'] = 'director'
                  found = True
              except:
                pass
            if not(found):
              try:
                if self.movie_dict[noun]:
                  noun_sentiment_dict['role'] = 'movie'
                  found = True
              except:
                pass
            if found:
              noun_sentiment_dict['sentiment'] = sentiment.polarity
              noun_sentiment_dict['value'] = noun
              criteria_list.append(noun_sentiment_dict)
          elif word in self.genre_list:
              noun_sentiment_dict['role'] = 'genre'
              found = True
              noun_sentiment_dict['value'] = word
              noun_sentiment_dict['sentiment'] = sentiment.polarity
              criteria_list.append(noun_sentiment_dict)
      return criteria_list



  def analyseSentiments(self,msg):
    '''
    we need to do pre processing before doing sentimentalAnalhysis like
    1. Spelling correction
    2. grammer correction
    3. Translate to english if in other language
    4. addiung punctuation if required! ( if we add this splitting might be easier)???

    '''

    #extract full tweets
    if 'extended_tweet' in msg['tweet_data'] :
        print( "extended_tweet")
        tweet_text = msg['tweet_data']['extended_tweet']['full_text']
        print ("extended_tweet",tweet_text)
    else:
        try:
          tweet_text = msg['tweet_data']['text']
        except:
          return
    userId = msg['tweet_data']["user"]["id_str"]
    punctuated_msg = str(tweet_text)

    
    #handle retweet case
    noun_sentiment_dict = {}
    criteria_list =[]
    output_data ={}
    if 'quoted_status' in msg['tweet_data']:
      criteria_list = self.computeNounTagsFromrtweetRelation(punctuated_msg,msg,noun_sentiment_dict)
    else:
      criteria_list = self.extractAndAssignPolarity(punctuated_msg.lower())

    print (criteria_list)
    output_data['type'] = 'sentimentsAnalyser'
    output_data['msg'] = criteria_list
    output_data['userid'] = userId

    Producer().produceMessage(json.loads(json.dumps(output_data)),"sentiments")
