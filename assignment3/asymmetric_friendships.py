import MapReduce
import sys

"""
Asymmetric Friendships
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

friends = {}

def mapper(record):
    # key: document identifier
    # value: document contents
    global friends
    key = record[1]
    value = record[0]
    words = value.split()

    for w in words:
      mr.emit_intermediate(w, key)

    if(friends.has_key(key)):
	 friends[key].append(value)
    else:
	 friends[key] = []
	 friends[key].append(value)
"""
    if(friends.has_key(key)):
         friends[key].append(value)
    else:
	 friends[key] = []
	 friends[key].append(value)"""
      

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    global friends
    for v in list_of_values:
      total = (key,v)
      if(friends.has_key(key) and v in friends[key]):
         continue 
      else:
	 mr.emit((total))
         turn = (v,key)
	 mr.emit((turn))






# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])  
    mr.execute(inputdata, mapper, reducer)

