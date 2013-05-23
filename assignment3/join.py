import MapReduce
import sys

"""
Implement a relational join as a MapReduce query
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[1]
    value = record
    words = key.split()
    for w in words:
      mr.emit_intermediate(w, value)    


def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = []
    count = 0
    for c in list_of_values:
	if("line_item" in c):
	   count += 1
    for i in range(count):
        items = []
	items = list_of_values[0]+list_of_values[i+1]
        mr.emit(items)


# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
