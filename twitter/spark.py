#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#pip install tweepy
#pip install pyspark
#pip install findspark


# In[1]:


# import libraries to visualize the results
import time
from IPython import display
import matplotlib.pyplot as plt
import seaborn as sns
import pandas
from textblob import TextBlob


# In[2]:


import findspark
findspark.init('C:\opt\spark')
# import necessary packages
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc


sc = SparkContext()
# we initiate the StreamingContext with 10 second batch interval. #next we initiate our sqlcontext
ssc = StreamingContext(sc, 10)
sqlContext = SQLContext(sc)

# initiate streaming text from a TCP (socket) source:
socket_stream = ssc.socketTextStream("127.0.0.1", 5555)
# lines of tweets with socket_stream window of size 60, or 60 #seconds windows of time
lines = socket_stream.window(10)

from collections import namedtuple

fields = ("hashtag", "count" )
Tweet = namedtuple( 'Tweet', fields )

fields2 = ("text", "count")
Sentiment = namedtuple('Sentiment', fields2)

# here we apply different operations on the tweets and save them to #a temporary sql table
( lines.flatMap( lambda text: text.split( "b'" ) ) #Splits to a list containing of the single tweets 
  .map( lambda word: ( word, 1) ) 
  .map( lambda rec: Sentiment( rec[0], rec[1] ) )
 # Sorts Them in a dataframe
  .foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") )
 # Registers only top 10 hashtags to a table.
  .limit(10).registerTempTable("Sentiment") ) )

# here we apply different operations on the tweets and save them to #a temporary sql table
( lines.flatMap( lambda text: text.split( " " ) ) #Splits to a list
  # Checks for    hashtag calls  
  .filter( lambda word: word.lower().startswith("#") ) 
  .map( lambda word: ( word.lower(), 1 ) ) # Lower cases the word
 # .reduceByKey( lambda a, b: a + b ) 
 # Stores in a Tweet Object
  .map( lambda rec: Tweet( rec[0], rec[1] ) )
 # Sorts Them in a dataframe
  .foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") )
 # Registers only top 10 hashtags to a table.
  .limit(10).registerTempTable("tweets") ) )


# In[ ]:


search = input("Enter searchword: ")
print("Search for: " + search)

#overwrite file

f = open("var.py", "w")
f.write("var1 = '%s'" % search)
f.close()

import subprocess

file = subprocess.Popen("job.sh", shell=True)

ssc.start()


# In[ ]:


print("Wait for data to stream...")
time.sleep(30)


# In[ ]:


#matplotlib inline
count = 0
while count < 5:
    
    time.sleep(5)
    # top hashtags:
    top_10_tags = sqlContext.sql( 'Select hashtag, count from tweets' )
    top_10_df = top_10_tags.toPandas()
    display.clear_output(wait=True)
    plt.figure( figsize = ( 10, 8 ) )
    sns.barplot( x="count", y="hashtag", data=top_10_df)
    plt.show()
    #sentiment analysis:
    top_10_senti = sqlContext.sql( 'Select text from sentiment' )
    top_10_s = top_10_senti.toPandas()
    print(top_10_s)
    count = count + 1
    print(count)
    # Schleife über alle texte, die in top_10_s gespeichert sind, und davon jeweils die sentiment analysis
    n = 9
    s = 0
    i = 1
    while i <= n:
        s = s + i
        opinion = TextBlob(top_10_s["text"][i])
        print(opinion.sentiment)
        i = i + 1


# In[ ]:




