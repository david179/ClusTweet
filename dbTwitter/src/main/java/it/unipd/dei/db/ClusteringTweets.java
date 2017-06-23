package it.unipd.dei.db;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ClusteringTweets {
    
	private static final String DRIVER = "org.postgresql.Driver";
	
	public static Dataset<Row> connectionDb(SparkSession spark){
		Dataset<Row> jdbcDF = spark.read()
				.format("jdbc")
				.option("url", "jdbc:postgresql://localhost/")
				.option("dbtable", "tweet_localusa")
				.option("user", "postgres")
				.option("password", "43286")
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
		Dataset<Twitter> tweetDb = jdbcDB.as(twitterEncoder);
		Dataset<Twitter> tweetDbPartial = tweetDb.limit(10000);
		
		tweetDb.show();
		try{
			Dataset<TwitterClustered> tweetClustered = KCenterMapReduce.cluster(tweetDbPartial, spark);
			tweetClustered.show();
		}
		catch(Exception e){
			System.out.println("\n\n***Unexpected error!");
		}
	}

}