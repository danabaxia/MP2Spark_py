	#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

delimeters = ''
stopwords = []

with open(stopWordsPath) as f:
	#TODO
    for word in f:
    	stopwords.append(word.strip())

with open(delimitersPath) as f:
    #TODO
    delimeters = f.readline()

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf = conf)

table = ''.maketrans(delimeters, ' '*len(delimeters))
word_counter = {}

lines = sc.textFile(sys.argv[3],1)
filtered_words = lines\
    .map(lambda line: line.lower())\
    .map(lambda line: line.translate(table))\
    .map(lambda line: line.split())\
    .flatMap(lambda word: word)\
    .fliter(lambda word: word not in stopwords)

word_counts = filtered_words\
    .map(lambda word: (word, 1))\
    .reduceByKey(add)\
    .collect()

word_counts.sort(key=lambda tuple: tuple[1])
word_counts.reverse()

top_10 = word_counts[:10]
top_10.sort()

#TODO

outputFile = open(sys.argv[4],"w")

#write results to output file. Foramt for each line: (line +"\n")
with open(sys.argv[4], "w") as outputFile:
	for tuple in top_10:
		outputFile.writelines(tuple[0] + '\t' + str(tuple[1]) + os.linesep)

sc.stop()
