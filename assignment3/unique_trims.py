import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line
textlist = []

def mapper(record):
    # key: document identifier
    # value: document contents
    global textlist 
    key = record[0]
    value = record[1]

    words = value.split()
    trim_text = words[0][:len(words[0])-10]    
    
    text = [trim_text] 

    for w in text:
      if (w in textlist):
          continue
      else:
          mr.emit_intermediate(key, w)
          textlist.insert(0,w)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    for v in list_of_values:
       mr.emit((v))



# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)

