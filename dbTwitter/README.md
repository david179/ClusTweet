### ClustTweet

ClustTweet is a project that uses the clustering tecqnique to group together tweets that have a similar content.
To do so, the tweets are loaded into a SQL database and then they are processed in Java with the Apache Spark Framework. The tweets are divided into groups by using a k-center clustering and the Word2Vec model then each cluster is analized to see how similar its tweets are in their contents.
To carry out the analysis The Stanford University Natural Language Processing Part of Speech Tagger has been used. For each tweet only the words tagged as noun are kept and the frequency of those words in their cluster is calculated. A set of most freqeunt noun is created for each cluster and for each of these frequent nouns the entropy is calculated in order to have an insight on the performance of the clustering algorithm. 


This was the final project for the exam Database Management Systems at The UNiversity of Padova - 7/20/2017

Authors:
Tommaso Agnolazza
Alessandro Ciresola
Davide Lucchi


### How To Use

### Postgre SQL
Check java version. 
If it is previous than java8 change postresql driver for java (jdbc) inside /libs/

The sql queries for creating the database and importing the cav files are inside /script/sql.
TODO: changing the path for the csv file which is inside the same folder.

### Compiling and executing with eclipse
1) Import the project as gradle project (it will handle all the external packages)
2) Modify the database information (database, username and password) inside the java class DbFunctions
3) Link the external library jdbs in /libs to the execution of the project
4) Executing

On default the program uses 8 core.
To modify this setting change the parametre .master on line 44 of file ClusTweets.java.

In order to execute again the code, please remove directory freq_nouns in the main directory and drop the "clusters" table in postgre database.

The output are three text files which will be written in the main directory.

### Compiling the code from command line
Execute the following commands in a shell
1) ./gradlew to download all the files needed
2) ./gradlew ShadowJar
3) ./script/java/executing.sh

### Documentation
The documentation can be found under the doc folder. It contains the Java Doc of all the classes that have been implemented

### Results
The results can be found under the results folder. The final project presentation is included as well. 
The results are three files:
Frequent nouns per cluster
Entropy per cluster
Entropy per frequent noun

The results are very promising and it can be noticed that there are many clusters that have tweets with very similar contents. Refer to the presentation for some examples.


