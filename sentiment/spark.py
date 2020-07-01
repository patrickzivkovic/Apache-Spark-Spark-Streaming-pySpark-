#!/usr/bin/env python
# coding: utf-8

# In[1]:


# import libraries
import time
from IPython import display
import matplotlib.pyplot as plt
import seaborn as sns
import pandas
from textblob import TextBlob
try:
    import subprocess
except:
    get_ipython().run_line_magic('pip', 'install subprocess')
    import subprocess
import numpy as np


# In[2]:


import findspark
findspark.init('C:\spark\spark')
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
lines = socket_stream.window(60)


# In[3]:


from collections import namedtuple

fields = ("hashtag", "count" )
Tweet = namedtuple( 'Tweet', fields )

fields2 = ("text", "count")
Sentiment = namedtuple('Sentiment', fields2)

# here we apply different operations on the tweets and save them to #a temporary sql table
( lines.flatMap( lambda text: text.split( "\n" ) ) #Splits to a list containing of the single tweets 
  .map( lambda word: ( word, 1) ) 
  .map( lambda rec: Sentiment( rec[0], rec[1] ) )
 # Sorts Them in a dataframe
  .foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") )
  .limit(50).registerTempTable("sentiment") ) )
    #  hier: alle texte müssen raus und schleife braucht viel zu lange...

# here we apply different operations on the tweets and save them to #a temporary sql table
( lines.flatMap( lambda text: text.split( " " ) ) #Splits to a list
  # Checks for    hashtag calls  
  .filter( lambda word: word.lower().startswith("#") ) 
  .map( lambda word: ( word.lower(), 1 ) ) # Lower cases the word
  .reduceByKey( lambda a, b: a + b ) 
 # Stores in a Tweet Object
  .map( lambda rec: Tweet( rec[0], rec[1] ) )
 # Sorts Them in a dataframe
  .foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") )
 # Registers only top 10 hashtags to a table.
  .limit(5).registerTempTable("tweets") ) )


# In[4]:


while True:
    search = input("Enter searchword: ")
    if(search != ""):
        print("Search for: " + search)
        break
    print("You have to use a searchword!")

search = search.split(",")

#overwrite file
f = open("var.py", "w")
f.write("search = %s" % search)
f.close()

file = subprocess.Popen("job.sh", shell = True)

ssc.start()


# In[ ]:


print("Wait 60 seconds stream to start properly...")
time.sleep(60)
print("...successfull")


# In[ ]:


def plot():
    try:
        error = 0
        get_ipython().run_line_magic('matplotlib', 'inline')
        while True:
            print("Gather Data for graph...")
            time.sleep(5)
            # top hashtags:
            top_10_tags = sqlContext.sql( 'Select hashtag, count from tweets' )
            top_10_df = top_10_tags.toPandas()
            print("successful")
            print("plotting")
            #display.clear_output(wait=True)
            plt.figure( figsize = ( 10, 8 ) )
            sns.barplot( x="count", y="hashtag", data=top_10_df)
            plt.show()
            sentiment()
    except:
        print("Data was not ready for printing....waiting....")
        time.sleep(20)
        error += 1
        if(error >= 5):
            print("Error on Streaming")
            ssc.stop()
            exit()
        plot()
        
def sentiment():
    get_ipython().run_line_magic('matplotlib', 'inline')
    error = 0
    try:
        print("Gater data for Sentiment Analysis...")
        #sentiment analysis:
        top_10_senti = sqlContext.sql( 'Select text from sentiment' )
        top_10_s = top_10_senti.toPandas()
        print("successful")
        #print(top_10_s)
        # Schleife über alle texte, die in top_10_s gespeichert sind, und davon jeweils die sentiment analysis
        n = 51
        positiveList = [] #empty list for storing positive tweets
        negativeList = [] #empty list for storing negative tweets
        countPosNeg = np.array([0, 0]) #array for counting positive and negative tweets --> 1 is positive, 0 is negative
        countNames = np.array(["negative", "positive"]) #names for plotting
        s = 0
        i = 1
        print("analyse text for sentiment...")
        while i <= n:
            opinion = TextBlob(top_10_s["text"][i])
            if(opinion.sentiment[0] == 0.0 and opinion.sentiment[1] == 0.0): #text is not suitable for sentiment
                i += 1
                s += 1
            else:
                if(s <= 5):
                    print("For the text: ", top_10_s["text"][i])
                    print("The text polarity is: %.2f" %opinion.sentiment[0])
                    print("The text subjectivity is: %.2f" %opinion.sentiment[1])
                    print("*****************************************************")
                if(opinion.sentiment[0] > 0):
                    countPosNeg[1] += 1 #add 1 to positive count because sentiment was positive
                    positiveList.append(opinion.sentiment[0])
                else:
                    countPosNeg[0] += 1 #add 1 to negatvie count because seintiment was negative
                    negativeList.append(opinion.sentiment[0])
                i += 1
                s += 1
        plt.figure( figsize = ( 10, 8 ))
        sns.barplot( x=countNames, y=countPosNeg)
        plt.show()
        
        plt.figure( figsize = ( 10, 8 ))
        sns.boxplot( y=countNames, x=[positiveList, negativeList])
        plt.show()
        if(input("continue?(y/n): ") == "y"):
            sentiment()
        else:
            ssc.stop()
            exit()
    except:
        print("Data was not ready for sentiment...waiting...")
        time.sleep(20)
        if(error >= 5):
            print("Error on sentiment, getting not enough data!")
            ssc.stop()
            exit()
        error += 1
        sentiment()


# In[ ]:


plot()


# In[ ]:





# In[ ]:




