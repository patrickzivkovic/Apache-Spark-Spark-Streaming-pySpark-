If execution of gui.ipynb does not work go throug these points and check after every one:
-----------------------------------------
1. Install Git for windows => https://gitforwindows.org/
   gets you Bash for Windows which is essential for job.sh
-----------------------------------------
2. Install Python (if not installed) => https://www.python.org/downloads/release/python-383/
   scroll all the way down to Windows x86 executable installer
-----------------------------------------
3. Python to Path
   go to Umgebungsvariablen -> Systemvariablen -> Path -> Neu -> inert path to your installed Python.exe (e.x C:\Users\chris\AppData\Local\Programs\Python\Python38-32)
-----------------------------------------
-----------------------------------------
############################
What are the files for:   ##
############################
gui.ipynb	##
########################################################################
is simply a one-liner for now. if you start you run the whole script  ##
########################################################################
job.sh	##
########################################################################
does the job of spyder. In spark.py (or .ipynb) job.sh is executet.   ##
Whole job is to call receive.py and to act like a pipeline for the    ##
incomine tiwtter data.                                                ##
########################################################################
receive.py	##
########################################################################
for the tweepy package. Is a twitter api which streams directly       ##
tiwtter data and filters it rudimentaly. Gets User input from         ##
var.py					                              ##
########################################################################
spark.py and (.ipynb)	##
########################################################################
is for doing the whole work. Gets tweepy data. Calls Spark.	      ##
Manipulates data in Spark's RDD's. Does sentiment analysison RDD's    ##
and plots it. Also reads in user input (filters searchword,...        ##
and stores variables in var.py.                                       ##
########################################################################
var.py	##
########################################################################
is for storing user input.					      ##
######################################################################## 