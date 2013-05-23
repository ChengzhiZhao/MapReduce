import MapReduce
import sys

"""
Multiply
"""
mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    global matrixA,matrixB
    # key: document identifier
    # value: document contents
    for i in range(5):
    	if(record[0]=='a'):
    		key = (record[1],i)
		mr.emit_intermediate(key, record)
    	else:
        	key = (i,record[2])
                mr.emit_intermediate(key, record)
    	
   

def reducer(key, list_of_values):
    total = 0
    matrixA = []
    matrixB = []
    for v in list_of_values:
        if(v[0]=='a'):
   	    matrixA.append(v)
	else:
	    matrixB.append(v)

    for A in matrixA:
	for B in matrixB:
	    if (A[2]==B[1]):
		total += A[3]*B[3]
    keylist = list(key)
    keylist.append(total)
    output = tuple(keylist)
    # key: word
    # value: list of occurrence counts
    mr.emit(output)





# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
