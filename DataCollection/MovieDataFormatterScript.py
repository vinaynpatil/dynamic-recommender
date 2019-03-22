import xlrd
from collections import OrderedDict
import simplejson as json

wb = xlrd.open_workbook('../MovieData/movie_metadata.xlsx')
sh = wb.sheet_by_index(0)

movie_list = []


for rownum in range(1, sh.nrows):
    movie = OrderedDict()
    row_values = sh.row_values(rownum)
    movie['movie_title'] = row_values[11].encode('ascii', 'ignore')
    movie['actor_1_name'] = row_values[10].encode('ascii', 'ignore')
    movie['actor_2_name'] = row_values[6].encode('ascii', 'ignore')
    genre_list= row_values[9].split("|")
    movie['genre'] = genre_list
    movie['director_name'] = row_values[1].encode('ascii', 'ignore')
    movie['duration'] = row_values[3]
    movie['plot_keywords'] = row_values[16].encode('ascii', 'ignore')
    
    movie['language'] = row_values[19].encode('ascii', 'ignore')
    movie['country'] = row_values[20].encode('ascii', 'ignore')
    movie['title_year'] = row_values[23]
    movie['imdb_score']  = row_values[25]
    movie_list.append(movie)


j = json.dumps(movie_list)

with open('../MovieData/movies_list.json', 'w') as f:
    f.write(j)