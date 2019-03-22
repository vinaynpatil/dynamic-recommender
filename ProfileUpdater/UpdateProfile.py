from kafka import KafkaConsumer
import json
from kafkaProducer import Producer
import time

class UpdateProfile:
  def consumerMessage(self):
      consumer = KafkaConsumer('sentiments',bootstrap_servers=['localhost:9092'])
      #consumer = KafkaConsumer('test',bootstrap_servers=['localhost:9092'],value_deserializer=lambda m: json.loads(m.decode('ascii')),auto_offset_reset='earliest', enable_auto_commit=False)
      for message in consumer:
        r_msg = str(message.value.decode("utf-8"))
        print ("consumed in UpdateProfile",r_msg)
        sentiment_data = json.loads(r_msg)
        self.searchAndUpdateDynamicProfile(sentiment_data)
        '''
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                            message.offset, message.key,
                                            message.value))
        '''

  def UpdateSubCriteria(self,userDict,criteria):
        with open("MovieData/movies_list_updated.json",'r') as m2f:
          #data = mf.read()
          self.movie_list = json.load(m2f)
        print ("UpdateSubCriteria")
        final_list =[]
        movie_name = criteria['value']
        for item in self.movie_list:
            if item['movie_handle'] == movie_name:
                output_dict ={}
                if item['actor_1_handle']:
                    output_dict['role'] = 'actor'
                    output_dict['sentiment'] = criteria['sentiment']
                    output_dict['value'] = item['actor_1_handle']
                    final_list.append(output_dict)
                if item['actor_2_handle']:
                    output_dict ={}
                    output_dict['role'] = 'actor'
                    output_dict['sentiment'] = criteria['sentiment']
                    output_dict['value'] = item['actor_2_handle']
                    final_list.append(output_dict)
                if item['director_handle']:
                    output_dict ={}
                    output_dict['role'] = 'director'
                    output_dict['sentiment'] = criteria['sentiment']
                    output_dict['value'] = item['director_handle']
                    final_list.append(output_dict)
                if len(item['genre']) > 0:
                    for gen in item['genre']:
                        output_dict ={}
                        print (gen)
                        output_dict['role'] = 'genre'
                        output_dict['sentiment'] = criteria['sentiment']
                        output_dict['value'] = gen
                        final_list.append(output_dict)
                break
        return final_list

  def searchAndUpdateDynamicProfile(self,sentiment_data):

      userId = sentiment_data['userid']
      criteria_list = sentiment_data['msg']

      with open("ProfileData/"+str(userId)+".json") as f:
        user_data = json.load(f)
      computation_duration = (1*60)
      print(userId,criteria_list)
      #user_data = profile_data['userId']
      updated_criteria =[]
      for criteria in criteria_list:
        if criteria['role'] =='movie':
          updated_criteria.extend(self.UpdateSubCriteria(user_data,criteria))

      print(updated_criteria)
      criteria_list.extend(updated_criteria)
      print(criteria_list)
      for criteria in criteria_list:
        print("Inside0")
        if criteria['role'] in user_data:
          foundcriteria = False
          print("Inside1")
          for key,val in user_data.items():
            if key == criteria['role']:
              print ("macthed criteria['value']",criteria['value'])
              foundcriteria = True
              key = criteria['value']
              if key in user_data[criteria['role']]:
                print (time.time() - user_data[criteria['role']][key]['time'])
                print(computation_duration)
                if (time.time() - user_data[criteria['role']][key]['time']) >= computation_duration:
                  if user_data[criteria['role']][key]['DP'] >= user_data[criteria['role']][key]['SP']:
                    print ("user_data[criteria['role']][key]['DP'] is greater than SP")
                    user_data[criteria['role']][key]['SP'] += abs((user_data[criteria['role']][key]['DP'])*(user_data[criteria['role']][key]['SP'])*(0.1))
                    if user_data[criteria['role']][key]['SP'] > 1:
                      user_data[criteria['role']][key]['SP'] = 1
                  else:
                    user_data[criteria['role']][key]['SP'] -= abs((user_data[criteria['role']][key]['DP'])*(user_data[criteria['role']][key]['SP'])*(0.1))
                    if user_data[criteria['role']][key]['SP'] < -1:
                      user_data[criteria['role']][key]['SP'] = -1

                print(user_data[criteria['role']][key])
                user_data[criteria['role']][key]['DP'] = criteria['sentiment'] 
                user_data[criteria['role']][key]['time'] = time.time()
                print(user_data[criteria['role']][key])
              else:
                user_data[criteria['role']][criteria['value']] = {}
                user_data[criteria['role']][criteria['value']]['SP'] = criteria['sentiment']
                user_data[criteria['role']][criteria['value']]['DP'] = criteria['sentiment']
                user_data[criteria['role']][criteria['value']]['time'] = time.time()
              break
          if foundcriteria == False:
             user_data[criteria['role']][criteria['value']] = {}
             user_data[criteria['role']][criteria['value']]['SP'] = criteria['sentiment']
             user_data[criteria['role']][criteria['value']]['DP'] = criteria['sentiment']
             user_data[criteria['role']][criteria['value']]['time'] = time.time()
          
        elif(criteria['role']!='movie'):
           #add criteria
           print ("aading criteria")
           user_data[criteria['role']] = {}
           user_data[criteria['role']][criteria['value']] = {}
           user_data[criteria['role']][criteria['value']]['SP'] = criteria['sentiment']
           user_data[criteria['role']][criteria['value']]['DP'] = criteria['sentiment']
           user_data[criteria['role']][criteria['value']]['time'] = time.time()
      with open("ProfileData/"+str(userId)+".json","w") as f:
        user_data = json.dumps(user_data)
        f.write(user_data)
      if sentiment_data['type'] == 'simulator':
        print ("producing sentimnts from triggersimulator")
        Producer().produceMessage(json.loads(json.dumps({'status':'ok'})),"triggersimulator")

UpdateProfile().consumerMessage()
