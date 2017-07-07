package it.unipd.dei.db;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import edu.stanford.nlp.ling.TaggedWord;
import it.unipd.dei.db.Utils.DbFunctions;
import scala.Tuple2;

public class ClusTweets {
	
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:\\winutil\\");
	    
		SparkSession spark = SparkSession
				.builder()
				.appName("ClusteringTweets")
				.master("local[8]")
				.getOrCreate();
		
		Dataset<Row> jdbcDB = DbFunctions.connectionDb(spark, "tweets_localusa");
		
		Encoder<Twitter> twitterEncoder = Encoders.bean(Twitter.class);
		Dataset<Twitter> tweetDb = jdbcDB.filter("country = 'US'").as(twitterEncoder);
		Dataset<Twitter> tweetDbPartial = tweetDb.limit(50000);
		
		//tweetDb.show();
		try{
	
			Dataset<TwitterClustered> tweetClustered = KCenterMapReduce.cluster(tweetDbPartial, spark);
			tweetClustered.show();
			
			
			DbFunctions.openConn();
			// insert the data into the output table
			/*
			 * 
			 * CREATE TABLE clusters(
				    tweet_ID varchar(150) ,
				    cluster int,
				    datetweet varchar(50),  
				    hour varchar(50),  
				    username varchar(50) ,  
				    nickname varchar(50),  
				    biography varchar(250),  
				    tweet_content text,  
				    favs varchar(150),  
				    rts varchar(50),  
				    latitude varchar(200) ,
				    longitude varchar(200) , 
				    country varchar(200) ,  
				    place varchar(200),  
				    profile_picture varchar(200),  
				    followers integer, 
				    following integer,  
				    listed integer,   
				    language varchar(10), 
				    url varchar(200),
				    primary key (tweet_ID) );
			 */
			tweetClustered.foreach( (tweet) -> {
					DbFunctions.insertTweet(tweet);
				}
			);
			
			DbFunctions.close();
			
			// Read the tweets and their cluster from the SQL clusters table
			jdbcDB = DbFunctions.connectionDb(spark, "clusters");
						
			Encoder<TwitterClustered> twitterEncoder2 = Encoders.bean(TwitterClustered.class);
			Dataset<TwitterClustered> tweetDb2 = jdbcDB.as(twitterEncoder2);
						
			JavaRDD<TwitterClustered> tweets = tweetDb2.toJavaRDD();
						
			System.out.println("The total number of tweets is: "+tweets.count());
			Clustering_evaluation_functions.start(tweets,spark);
		}
		catch(Exception e){
			e.printStackTrace();
			System.out.println("\n\n***Unexpected error!");
		}
	}

}