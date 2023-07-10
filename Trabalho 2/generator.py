from random import *

# Const variables that defines the number of strings generated
maxQueries = 100000
maxBases = 10

string = ">Query string #"
tmpStr = ""
bases = ['A', 'C', 'T', 'G']
index = 0
path='./query.in'
queryFile = open(path,'w')

for i in range(maxQueries):
    tmpStr = string + str(i)
    queryFile.write(tmpStr)
    queryFile.write("\n")
    tmpStr = ""
    for j in range(randint(0,999)):
        index = randint(0,3)
        tmpStr = tmpStr + bases[index]
    queryFile.write(tmpStr)
    queryFile.write("\n")

queryFile.close()

path='./dna.in'
baseFile = open(path,'w')
string = "> Escherichia coli K-12 MG1655 section "
for i in range(maxBases):
    tmpStr = string + str(i + 1)
    baseFile.write(tmpStr)
    baseFile.write("\n")
    tmpStr = ""
    for j in range(125):
        tmpStr = ""
        for k in range (80):
            index = randint(0,3)
            tmpStr = tmpStr + bases[index]
        baseFile.write(tmpStr + "\n")
        # baseFile.write("\n")


baseFile.close()