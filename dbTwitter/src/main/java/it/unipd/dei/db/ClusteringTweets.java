package it.unipd.dei.db;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import it.unipd.dei.db.Utils.DbFunctions;
import scala.Tuple2;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.ws4j.RelatednessCalculator;
import edu.cmu.lti.ws4j.impl.Lin;
import edu.stanford.nlp.ling.SentenceUtils;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import edu.cmu.lti.lexical_db.*;

public class ClusteringTweets {
    
	private static final String DRIVER = "org.postgresql.Driver";
	private static ILexicalDatabase db = new NictWordNet();
	private static RelatednessCalculator lin = new Lin(db);
	
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
		
		/*Dataset<Row> jdbcDB = connectionDb(spark);
		
		Encoder<Twitter> twitterEncoder = Encoders.bean(Twitter.class);
		Dataset<Twitter> tweetDb = jdbcDB.filter("country = 'US'").as(twitterEncoder);
		Dataset<Twitter> tweetDbPartial = tweetDb.limit(5000);
		*/
		//tweetDb.show();
		try{
			
			//Dataset<TwitterClustered> tweetClustered = KCenterMapReduce.cluster(tweetDbPartial, spark);
			//tweetClustered.show();
			
			
			//DbFunctions.openConn();
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
			/*tweetClustered.foreach( (tweet) -> {
					DbFunctions.insertTweet(tweet);
				}
			);
			
			DbFunctions.close();*/
			
			
			Dataset<Row> jdbcDB = spark.read()
					.format("jdbc")
					.option("url", "jdbc:postgresql://localhost/")
					.option("dbtable", "clusters")
					.option("user", "postgres")
					.option("password", "pass")
					.load();
			
			Encoder<Twitter_cluster> twitterEncoder2 = Encoders.bean(Twitter_cluster.class);
			Dataset<Twitter_cluster> tweetDb2 = jdbcDB.as(twitterEncoder2);
			
			JavaRDD<Twitter_cluster> tweets = tweetDb2.toJavaRDD();
			
			System.out.println(tweets.count());
			
			JavaRDD<Tuple2<Integer,String>> c = tweets.map((tweet) -> {
				
				return new Tuple2<Integer,String>(tweet.getcluster(),tweet.getTweet_content());
				
			});
			
			JavaPairRDD<Integer,String> c2 = JavaPairRDD.fromJavaRDD(c);
			
			JavaPairRDD<Integer,Iterable<String>> clusters = c2.groupByKey();
			
			
			JavaRDD<Tuple2<Integer,ArrayList<List<TaggedWord>>>> m = clusters.map((tuple) -> {
		    	
		    	Iterator<String> it = tuple._2.iterator();
		        
		    	Scanner s;
				List<List<Word>> sentences = new ArrayList();
				while (it.hasNext())
				{
					s = new Scanner(it.next());
					s.useDelimiter(" ");
					List<Word> l = new ArrayList<Word>();
					
					while (s.hasNext())
					{
						l.add(new Word(s.next()));
					}
					sentences.add(l);
				}
				
				MaxentTagger tagger = new MaxentTagger("models/english-left3words-distsim.tagger");
			    ArrayList<List<TaggedWord>> list = new ArrayList();
			    for (List<Word> sentence : sentences) {
			      List<TaggedWord> tSentence = tagger.tagSentence(sentence);
			      //System.out.println(SentenceUtils.listToString(tSentence, false));
			      list.add(tSentence);
			    }
			    
		        
		        return new Tuple2<Integer,ArrayList<List<TaggedWord>>>(tuple._1,list);
		    });
			
			
			JavaRDD<Tuple2<Integer,Map<TaggedWord,Double>>> freq_clust  = m.map((tuple) -> {
				
				ArrayList<List<TaggedWord>> list = tuple._2();
				Map<TaggedWord,Double> freq = new HashMap();
				
				for (int i = 0; i < list.size(); i++)
				{
					Iterator<TaggedWord> it = list.get(i).iterator();
					while(it.hasNext())
					{
						TaggedWord w = it.next();
						
						String[] tags = {"NN","NNS","NNP","NNPS"};
						for (int j = 0; j < tags.length; j++)
						{
							if (w.tag().compareTo(tags[j]) == 0)
							{
								if (!freq.containsKey(w))
								{
									freq.put(w,(double)1);
								}
								else
								{
									double v = freq.get(w);
									freq.put(w, v+1);
								}
							}
						}
					}
				}
				
				Iterator<Map.Entry<TaggedWord,Double>> it = freq.entrySet().iterator();
				while(it.hasNext())
				{
					Map.Entry<TaggedWord,Double> w = it.next();
					
					double v = w.getValue()/(double)list.size();
					if (v >= 0.1)
						freq.put(w.getKey(), v);
					else
						it.remove();
				}
				
				return new Tuple2<Integer,Map<TaggedWord,Double>>(tuple._1(),freq);
			});
			
			
			
			freq_clust.saveAsTextFile("freq.txt");
			
		}
		catch(Exception e){
			e.printStackTrace();
			System.out.println("\n\n***Unexpected error!");
		}
	}

}