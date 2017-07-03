package it.unipd.dei.db;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.feature.Word2VecModel;
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
		
		Dataset<Row> jdbcDB = connectionDb(spark);
		
		Encoder<Twitter> twitterEncoder = Encoders.bean(Twitter.class);
		Dataset<Twitter> tweetDb = jdbcDB.filter("country = 'US'").as(twitterEncoder);
		Dataset<Twitter> tweetDbPartial = tweetDb.limit(50000);
		
		//tweetDb.show();
		try{
	
			//Dataset<TwitterClustered> tweetClustered = KCenterMapReduce.cluster(tweetDbPartial, spark);
			//tweetClustered.show();
			
			
			//DbFunctions.openConn();
			// insert the data into the output table
			/*
			 * 
			 * CREATE TABLE clusters_150_v50(
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
			
			jdbcDB = spark.read()
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
			
			
			JavaRDD<Tuple2<Integer,ArrayList<List<TaggedWord>>>> clusters_tagged = clusters.map((tuple) -> {
		    	
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
			
			
			/*JavaRDD<Tuple2<Integer,Map<TaggedWord,Double>>> freq_clust  = clusters_tagged.map((tuple) -> {
				
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
					if (v >= 0.25)
						freq.put(w.getKey(), v);
					else
						it.remove();
				}
				
				return new Tuple2<Integer,Map<TaggedWord,Double>>(tuple._1(),freq);
			});
			
			
			
			//freq_clust.saveAsTextFile("freq_300_050.txt");
			freq_clust.saveAsObjectFile("freq_nouns");
			*/
			JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
			sc.setLogLevel("OFF");
			
			JavaRDD<Tuple2<Integer,Map<TaggedWord,Double>>> 
			freq_clust = sc.objectFile("freq_nouns");
		
			
			//JavaRDD<Tuple2<Integer,Map<TaggedWord,Double>>> freq_clust
			JavaRDD<Tuple2<Integer,ArrayList<Tuple2<TaggedWord,Double>>>> top_nouns_per_cluster = freq_clust.map((tuple) -> {
				
				Map<TaggedWord,Double> freq_nouns = tuple._2;
				
				ArrayList<Tuple2<TaggedWord,Double>> most_freq_nouns = new ArrayList();
				TaggedWord one = null;
				TaggedWord two = null;
				for (TaggedWord w : freq_nouns.keySet())
				{
					if (one == null)
					{
						one = w;
					}
					else if (two == null)
					{
						two = w;
					}
					else if (freq_nouns.get(w) > freq_nouns.get(one))
					{
						one = w;
					}
					else if (freq_nouns.get(w) > freq_nouns.get(two))
					{
						two = w;
					}
				}
				if (one != null)
					most_freq_nouns.add(new Tuple2(one,freq_nouns.get(one)));
				//if (two != null)
					//most_freq_nouns.add(new Tuple2(two,freq_nouns.get(two)));
				
				return new Tuple2(0,most_freq_nouns);
			});
			
			JavaPairRDD<Integer,ArrayList<Tuple2<TaggedWord,Double>>>  tmp = JavaPairRDD.fromJavaRDD(top_nouns_per_cluster).reduceByKey((tuple1,tuple2)->{
				
				ArrayList<Tuple2<TaggedWord,Double>> a1 = tuple1;
				ArrayList<Tuple2<TaggedWord,Double>> a2 = tuple2;
				
				ArrayList<Tuple2<TaggedWord,Double>> tot = new ArrayList();
				for (int i = 0; i < a1.size(); i++)
				{
					tot.add(a1.get(i));
				}
				for (int i = 0; i < a2.size(); i++)
				{
					tot.add(a2.get(i));
				}
				
				return tot;
			});
			
			ArrayList<Tuple2<TaggedWord,Double>> freq_nouns = tmp.first()._2();
			
			
			// calculate cluster entropy
			/*
			 * 
			 * Sum -Nci/Nc log(Nci/Nc)
			 * 
			 * where: 
			 * 		Nci = # of tags i inside cluster C
			 * 		Nc = # of tags in cluster C
			 */
			
			//Distribute frequent nouns in broadcast to the workers
			/*final Broadcast<ArrayList<Tuple2<TaggedWord,Double>>> freq_nouns_br = sc.broadcast(freq_nouns);
		 
			JavaRDD<Tuple2<Integer,Double>> entropy_per_cluster = clusters_tagged.map((tuple) ->{
				
				ArrayList<Tuple2<TaggedWord,Double>> freq_nouns_total = freq_nouns_br.getValue();
				
				ArrayList<List<TaggedWord>> tags_in_cluster = tuple._2();
				
				Map<TaggedWord,Integer> noun_tags = new HashMap();
				// filter tags in order to keep only nouns
				int total = 0;
				for (int i = 0; i < tags_in_cluster.size(); i++)
				{
					Iterator<TaggedWord> it = tags_in_cluster.get(i).iterator();
					while(it.hasNext())
					{
						TaggedWord w = it.next();
						
						String[] tags = {"NN","NNS","NNP","NNPS"};
						for (int j = 0; j < tags.length; j++)
						{
							if (w.tag().compareTo(tags[j]) == 0)
							{
								if (!noun_tags.containsKey(w))
								{
									noun_tags.put(w,1);
								}
								else
								{
									int v = noun_tags.get(w);
									noun_tags.put(w, v+1);
								}
								total++;
							}
						}
					}
				}
				
				System.out.println("TOTAL is: "+total);
				
				Map<TaggedWord,Double> cluster_entropy = new HashMap();
				double entropy = 0;
				//calculate entropy
				for (Tuple2<TaggedWord,Double> t : freq_nouns_total)
				{
					// calculate number of occurences of this tag inside the cluster
					Integer occurences = noun_tags.get(t._1());
					if (occurences != null)
					{
						double o = occurences.intValue();
						if (o > 0)
							entropy += (o/total) * Math.log(o/total)*(-1);
						//System.out.println(entropy+" occ: "+o+", total: "+total);
					}
				}
				
				return new Tuple2<Integer,Double>(tuple._1(),entropy);
			});
			
			System.out.println("The max is log2 L: "+Math.log(freq_nouns.size()));
			entropy_per_cluster.saveAsTextFile("entropy_per_cluster.txt");
			
			*/
			
			
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
			final Broadcast<ArrayList<Tuple2<TaggedWord,Double>>> freq_nouns_br = sc.broadcast(freq_nouns);
		 
			Map<TaggedWord,Integer> tags_and_total_count = JavaPairRDD.fromJavaRDD(clusters_tagged.map((tuple) ->{
				
				ArrayList<Tuple2<TaggedWord,Double>> freq_nouns_total = freq_nouns_br.getValue();
				
				//System.out.print("freq nouns total "+freq_nouns_total.size());
				ArrayList<List<TaggedWord>> tags_in_cluster = tuple._2();
				
				Map<TaggedWord,Integer> noun_tags = new HashMap();
				// filter tags in order to keep only nouns
				int total = 0;
				for (int i = 0; i < tags_in_cluster.size(); i++)
				{
					Iterator<TaggedWord> it = tags_in_cluster.get(i).iterator();
					while(it.hasNext())
					{
						TaggedWord w = it.next();
						
						String[] tags = {"NN","NNS","NNP","NNPS"};
						for (int j = 0; j < tags.length; j++)
						{
							if (w.tag().compareTo(tags[j]) == 0)
							{
								if (!noun_tags.containsKey(w))
								{
									noun_tags.put(w,1);
								}
								else
								{
									int v = noun_tags.get(w);
									noun_tags.put(w, v+1);
								}
								total++;
							}
						}
					}
				}
				
				return new Tuple2<Integer,Map<TaggedWord,Integer>>(0,noun_tags);
				
			})).reduceByKey((tuple1,tuple2) ->{
				
				Map<TaggedWord,Integer> a1 = tuple1;
				Map<TaggedWord,Integer> a2 = tuple2;
				
				Map<TaggedWord,Integer> tot = new HashMap();
				for (TaggedWord w : a1.keySet())
				{
					tot.put(w, a1.get(w));
				}
				for (TaggedWord w : a2.keySet())
				{
					if (tot.containsKey(w))
					{
						tot.put(w, tot.get(w)+a2.get(w));
					}
					else
					{
						tot.put(w, a2.get(w));
					}
				}
				
				return tot;
			}).first()._2;
			
			
			//Distribute frequent nouns in broadcast to the workers
			//freq_nouns_br = sc.broadcast(freq_nouns);
			Broadcast<Map<TaggedWord,Integer>> tags_tot_count = sc.broadcast(tags_and_total_count);
			
			System.out.println("size: "+tags_and_total_count.size());
			
			//JavaRDD<Tuple2<Integer,ArrayList<List<TaggedWord>>>> clusters_tagged = 
			Map<TaggedWord,Double> entropy_per_noun = JavaPairRDD.fromJavaRDD(clusters_tagged.map((tuple) ->{
				
				ArrayList<Tuple2<TaggedWord,Double>> freq_nouns_total = freq_nouns_br.getValue();
				Map<TaggedWord,Integer> tags_tot_count2 = tags_tot_count.value();
				
				ArrayList<List<TaggedWord>> tags_in_cluster = tuple._2();
				
				Map<TaggedWord,Integer> noun_tags = new HashMap();
				// filter tags in order to keep only nouns
				//int total = 0;
				for (int i = 0; i < tags_in_cluster.size(); i++)
				{
					Iterator<TaggedWord> it = tags_in_cluster.get(i).iterator();
					while(it.hasNext())
					{
						TaggedWord w = it.next();
						
						String[] tags = {"NN","NNS","NNP","NNPS"};
						for (int j = 0; j < tags.length; j++)
						{
							if (w.tag().compareTo(tags[j]) == 0)
							{
								if (!noun_tags.containsKey(w))
								{
									noun_tags.put(w,1);
								}
								else
								{
									int v = noun_tags.get(w);
									noun_tags.put(w, v+1);
								}
								//total++;
							}
						}
					}
				}
				
				//System.out.println("TOTAL is: "+total);
				
				Map<TaggedWord,Double> cluster_entropy = new HashMap();
				//calculate entropy
				Map<TaggedWord,Double> out = new HashMap();
				for (Tuple2<TaggedWord,Double> t : freq_nouns_total)
				{
					double entropy = 0;
					// calculate number of occurences of this tag inside the cluster
					Integer occurences = noun_tags.get(t._1());
					double total = tags_tot_count2.get(t._1);
					if (occurences != null)
					{
						double o = occurences.intValue();
						if (o > 0)
							entropy = (o/total) * Math.log(o/total)*(-1);
						//System.out.println(entropy+" occ: "+o+", total: "+total);
					}
					cluster_entropy.put(t._1, entropy);
				}
				
				
				return new Tuple2<Integer,Map<TaggedWord,Double> >(0,cluster_entropy);
			})).reduceByKey((tuple1,tuple2)->{
				
				Map<TaggedWord,Double> a1 = tuple1;
				Map<TaggedWord,Double> a2 = tuple2;
				
				Map<TaggedWord,Double> tot = new HashMap();
				for (TaggedWord w : a1.keySet())
				{
					if (tot.containsKey(w))
					{
						tot.put(w, tot.get(w)+a1.get(w));
					}
					else
					{
						tot.put(w, a1.get(w));
					}
				}
				for (TaggedWord w : a2.keySet())
				{
					if (tot.containsKey(w))
					{
						tot.put(w, tot.get(w)+a2.get(w));
					}
					else
					{
						tot.put(w, a2.get(w));
					}
				}
				
				return tot;
			}).first()._2();
			
			System.out.print("final size "+entropy_per_noun.size());
			
			for(TaggedWord w : entropy_per_noun.keySet())
			{
				System.out.println("Word: "+w.value()+", entropy: "+entropy_per_noun.get(w));
			}
			//JavaRDD<Tuple2<Integer,Map<TaggedWord,Double> >> entropy_per_noun_per_cluster
			
			System.out.println("The max is log2 L: "+Math.log(150));
			//entropy_per_cluster.saveAsTextFile("entropy_per_cluster.txt");
			
		}
		catch(Exception e){
			e.printStackTrace();
			System.out.println("\n\n***Unexpected error!");
		}
	}

}