from googlesearch 
import search

import json

with open('movies_list.json')
as f:

data = json.load(f)



with open('actor_2_handle_list.json','w')
as f1:

for movie 
in data:

name = movie['actor_2_name']

if(name!=''):

print("Fetching : " + name)

f1.write(name + "@@@" + 
list(search('official twitter account '+name,
tld="co.in",
num=1, 
stop=1, 
pause=2))[0])

f1.write("\n")




