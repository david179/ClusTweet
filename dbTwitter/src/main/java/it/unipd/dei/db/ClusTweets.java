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
		Dataset<Twitter> tweetDbPartial = tweetDb.limit(50000);
		
		//tweetDb.show();
		try{
	
			/*Dataset<TwitterClustered> tweetClustered = KCenterMapReduce.cluster(tweetDbPartial, spark);
			tweetClustered.show();
			
			
			DbFunctions.openConn();*/
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
			/*tweetClustered.foreach( (tweet) -> {
					DbFunctions.insertTweet(tweet);
				}
			);
			
			DbFunctions.close();
			*/
			
			
			// Read the tweets and their cluster from the SQL clusters table
			jdbcDB = spark.read()
					.format("jdbc")
					.option("url", "jdbc:postgresql://localhost/")
					.option("dbtable", "clusters")
					.option("user", "postgres")
					.option("password", "pass")
					.load();
			
			Encoder<TwitterClustered> twitterEncoder2 = Encoders.bean(TwitterClustered.class);
			Dataset<TwitterClustered> tweetDb2 = jdbcDB.as(twitterEncoder2);
			
			JavaRDD<TwitterClustered> tweets = tweetDb2.toJavaRDD();
			
			System.out.println("The total number of tweets is: "+tweets.count());
			
			// Map each tweet into a tuple where the Integer is the cluster number and the String is the tweet content
			JavaRDD<Tuple2<Integer,String>> c = tweets.map((tweet) -> {
				return new Tuple2<Integer,String>(tweet.getCluster(),tweet.getTweet_content());
			});
			
			// convert to javaPair
			JavaPairRDD<Integer,String> c2 = JavaPairRDD.fromJavaRDD(c);
			
			JavaPairRDD<Integer,Iterable<String>> clusters = c2.groupByKey();
			
			// for each cluster tag all the words in its tweet. In other words to each tweet's word a tag is assigned
			// indicating whether the word is a noun, adjective, verb, ...
			JavaRDD<Tuple2<Integer,ArrayList<List<TaggedWord>>>> clusters_tagged = Clustering_evaluation_functions.tag_tweets(clusters);	
			
			
			
			// For each cluster keep only the words tagged as nouns as they are supposed to carry most of the information in the tweet
			// Integer is the cluster number
			// TaggedWord is a word tagged as noun
			// Double is the number of occurrences of that word in the current cluster
			// Integer is the total number of tags in this cluster
			JavaRDD<Tuple2<Integer,Tuple2<Map<TaggedWord,Integer>,Integer>>> tags_per_cluster_and_total_count = Clustering_evaluation_functions.filter_nouns(clusters_tagged);
				
			
			
			// Find the most frequent nouns inside each cluster.
			// Integer is the cluster number
			// TaggedWord is the tagged word
			// Double is the frequency of the tagged word in the Integer cluster
			JavaRDD<Tuple2<Integer,Map<TaggedWord,Double>>> freq_clust  = Clustering_evaluation_functions.frequent_nouns(tags_per_cluster_and_total_count, 0.25);
			
			
			// save the frequent nouns object so it can be loaded without being recomputed every time
			freq_clust.saveAsObjectFile("freq_nouns");
		
			PrintWriter print = new PrintWriter(new File("Frequent_nouns_per_cluster.txt"));
			Iterator<Tuple2<Integer,Map<TaggedWord,Double>>> iterator = freq_clust.collect().iterator();
			while (iterator.hasNext())
			{
				Tuple2<Integer,Map<TaggedWord,Double>> tuple = iterator.next();
				int cluster = tuple._1();
				Map<TaggedWord,Double> words = tuple._2();
				
				String out = "\nCluster number: "+cluster+"\n";
				for (TaggedWord w : words.keySet())
				{
					out += "\t"+w.value()+" - "+words.get(w)+"\n";
				}
				print.println(out);
			}
			print.close();
			
			
			JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
			sc.setLogLevel("OFF");
			
			// load the saved model from memory
			//JavaRDD<Tuple2<Integer,Map<TaggedWord,Double>>> 
			freq_clust = sc.objectFile("freq_nouns");
		
			
			// Only a subset of the most frequent nouns per cluster will appear in the final list of most frequent nouns
			// In this case only the most frequent noun per cluster is kept
			// Arraylist with the most frequent nouns
			ArrayList<Tuple2<TaggedWord,Double>> freq_nouns  = Clustering_evaluation_functions.filtered_frequent_nouns(freq_clust);
			
		
			// calculate cluster entropy
			/*
			 * 
			 * Sum -Nci/Nc log(Nci/Nc)
			 * 
			 * where: 
			 * 		Nci = # of tags i inside cluster C
			 * 		Nc = # of tags in cluster C
			 */
			JavaRDD<Tuple2<Integer,Double>> entropy_per_cluster = Clustering_evaluation_functions.cluster_entropy(sc, clusters_tagged, freq_nouns);
			entropy_per_cluster.saveAsTextFile("entropy_cluster.txt");
			// save on disk to see the results
			print = new PrintWriter(new File("Entropy_per_cluster.txt"));
			Iterator<Tuple2<Integer,Double>> it = entropy_per_cluster.collect().iterator();
			while (it.hasNext())
			{
				Tuple2<Integer,Double> tuple = it.next();
				int cluster = tuple._1();
				Double entropy = tuple._2();
				
				print.println("\nCluster number: "+cluster+",\tentropy: "+entropy);
			}
			print.close();
			
			
			// calculate noun entropy
			/*
			 * 
			 * Sum -Nci/Ni log(Nci/Ni)
			 * 
			 * where: 
			 * 		Nci = # of tags i inside cluster C
			 * 		Ni = # of tags i
			 */
			//Distribute frequent nouns in broadcast to the workers
			Map<TaggedWord,Double> entropy_per_noun = Clustering_evaluation_functions.frequent_nouns_entropy(sc, clusters_tagged, freq_nouns);
		
			// save to disk
			print = new PrintWriter(new File("Entropy_per_frequent_noun.txt"));
			print.println("Entropy for each frequent noun\nMinimum value = 0, Maximum value = "+Math.log(150));
			for(TaggedWord w : entropy_per_noun.keySet())
			{
				print.println("Word: "+w.value()+", entropy: "+entropy_per_noun.get(w));
			}
			print.close();
			
		}
		catch(Exception e){
			e.printStackTrace();
			System.out.println("\n\n***Unexpected error!");
		}
	}

}