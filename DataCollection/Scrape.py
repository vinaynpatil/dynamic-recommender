from googlesearch import search
import json
with open('../MovieData/movies_list.json') as f:
    data = json.load(f)

with open('../MovieData/actor_1_handle_list.json','w') as f1:
    for movie in data:
        name = movie['actor_1_name']
        if(name!=''):
            print("Fetching : " + name)
            f1.write(name + "@@@" + list(search('official twitter account '+name, tld="co.in", num=1, stop=1, pause=2))[0])
            f1.write("\n")
