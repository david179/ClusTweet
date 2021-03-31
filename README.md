## ClustTweet

ClustTweet uses the clustering technique to group together tweets that have a similar content. The program loads a dataset of tweets into a SQL database and processes them using the Apache Spark Framework. The tweets are grouped using a k-center clustering algorithm that uses the Word2Vec model to group them based on their content and each cluster is analyzed to determine the similarity of its tweets using the Stanford University Natural Language Processing Part of Speech Tagger. For each tweet within a cluster, only the words tagged as noun are kept and their frequency calculated. In this way, a set of most frequent nouns is calculated along with their entropy to determine the performance of the clustering algorithm. 

### Dependencies

ClustTweet requires the following components:
* A local Apache Hadoop installation 
* A PostgreSQL server and JDBC Driver
* A dataset of tweets

### Set Up

#### PostgreSQL
The database is used to store the dataset and provide a mean to Apache Spark to read it and store results. A SQL script is provided in [script/sql](script/sql) to create the tables required by ClustTweet and load the dataset into the database. 

Before running the SQL script, open it and update the path to the dataset that is found in the options of the command that creates the `tweet_usa` table. A sample dataset is provided in the [Dataset folder](Dataset).

Once the path is updated, run the script on your PostgresSQL database.

#### ClustTweet Configuration

The property file `clusttweet.properties` includes the configurable properties used by ClustTweet. The set of properties is:
```txt
hadoop.home.dir=D:\\hadoop\\hadoop-3.2.1\\
database.uri=jdbc:postgresql://localhost:5433/
database.user=postgres
database.password=123abcz
word.tagger=edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger
evaluation.entropy.noun=Entropy_per_frequent_noun.txt
evaluation.entropy.cluster=Entropy_per_cluster.txt
evaluation.frequent.nouns=Frequent_nouns_per_cluster.txt
```
where:
* hadoop.home.dir - Is the home directory for Hadoop
* database.uri - Is the uri to connect to the PostgreSQL database
* database.user - Is the user of the PostgreSQL database
* database.password - Is the password for the PostgreSQL database
* word.tagger - Is the path to the Natural Language Processing Part of Speech Tagger
* evaluation.entropy.noun - Is the path to an output file where ClustTweet will write the entropy calculated for each most frequent noun
* evaluation.entropy.cluster - Is the path to an output file where ClustTweet will write the entropy calculated for each cluster
* evaluation.frequent.nouns - Is the path to an output file where ClustTweet will write the most frequent nouns found in each cluster

### Compiling and executing with eclipse
1) Import the project as a gradle project 
2) Update the properties in `clusttweet.properties`
3) Add the JDBC driver for PostgreSQL to the classpath
4) Run the project as a Java Application
5) Inspect the `evaluation` files configured in `clusttweet.properties` to explore the output of ClustTweet


In order to execute again the code,  drop the "clusters" table in postgre database.


### Documentation
A Java Doc documentation is included in the [doc](doc) folder. 

### Results
The results obtained when running ClustTweet against the included dataset can be found in the [results folder](Results). 