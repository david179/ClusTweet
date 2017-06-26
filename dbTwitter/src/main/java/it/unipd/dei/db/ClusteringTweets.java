package it.unipd.dei.db;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import it.unipd.dei.db.Utils.DbFunctions;
import scala.Tuple2;
import java.util.Iterator;

public class ClusteringTweets {
    
	private static final String DRIVER = "org.postgresql.Driver";
	
	public static Dataset<Row> connectionDb(SparkSession spark){
		Dataset<Row> jdbcDF = spark.read()
				.format("jdbc")
				.option("url", "jdbc:postgresql://localhost/")
				.option("dbtable", "tweet_localusa2")
				.option("user", "postgres")
				.option("password", "pass")
				.load();
		return jdbcDF;
	}
	
	public static void main(String[] args) {
		
		//Registration of the driver that must be used 
	    try{
	    	Class.forName(DRIVER); 
	    	System.out.println("Driver " + DRIVER + " has been correctly registered."); 
	    }catch(ClassNotFoundException e) {
	    	System.out.println("Driver " + DRIVER + " not found." ); 
	        System.out.println("Error: " + e.getMessage()); 
	    	System.exit(-1); 

	    }
	    
		SparkSession spark = SparkSession
				.builder()
				.appName("ClusteringTweets")
				.master("local[8]")
				.getOrCreate();
		
		Dataset<Row> jdbcDB = connectionDb(spark);
		
		Encoder<Twitter> twitterEncoder = Encoders.bean(Twitter.class);
		Dataset<Twitter> tweetDb = jdbcDB.filter("country = 'US'").as(twitterEncoder);
		Dataset<Twitter> tweetDbPartial = tweetDb.limit(30000);
		
		tweetDb.show();
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
				    primay key (tweet_ID) );
			 */
			tweetClustered.foreach( (tweet) -> {
					DbFunctions.insertTweet(tweet);
				}
			);
			
			DbFunctions.close();
			
		}
		catch(Exception e){
			e.printStackTrace();
			System.out.println("\n\n***Unexpected error!");
		}
	}

}