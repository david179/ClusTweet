package it.unipd.dei.db;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import it.unipd.dei.db.Utils.Lemmatizer;
import scala.Tuple2;

/**
 * This class contains methods to evaluate the quality of the clustering
 * 
 * @author Tommaso Agnolazza
 * @author Alessandro Ciresola
 * @author Davide Lucchi
 */
public class Clustering_evaluation_functions {
	
	
	/**
	 * This method contains all the calls to the function to calculate the frequent nouns, the cluster entropy and the nouns entropy
	 * 
	 * @param tweets The list of tweets with their cluster number
	 * @param spark The spark session object
	 */
	public static void start(JavaRDD<TwitterClustered> tweets, SparkSession spark){
		try{	
			// Map each tweet into a tuple where the Integer is the cluster number and the String is the tweet content
			JavaRDD<Tuple2<Integer,String>> c = tweets.map((tweet) -> {
				return new Tuple2<Integer,String>(tweet.getCluster(),tweet.getTweet_content());
			});
			
			// convert to javaPair
			JavaPairRDD<Integer,String> c2 = JavaPairRDD.fromJavaRDD(c);
			
			JavaPairRDD<Integer,Iterable<String>> clusters = c2.groupByKey();
			
			// for each cluster tag all the words in its tweet. In other words to each tweet's word a tag is assigned
			// indicating whether the word is a noun, adjective, verb, ...
			JavaRDD<Tuple2<Integer,ArrayList<List<TaggedWord>>>> clusters_tagged = tag_tweets(clusters);	
			
			
			// For each cluster keep only the words tagged as nouns as they are supposed to carry most of the information in the tweet
			// Integer is the cluster number
			// TaggedWord is a word tagged as noun
			// Double is the number of occurrences of that word in the current cluster
			// Integer is the total number of tags in this cluster
			JavaRDD<Tuple2<Integer,Tuple2<Map<TaggedWord,Integer>,Integer>>> tags_per_cluster_and_total_count = filter_nouns(clusters_tagged);
			
			// Find the most frequent nouns inside each cluster.
			// Integer is the cluster number
			// TaggedWord is the tagged word
			// Double is the frequency of the tagged word in the Integer cluster
			JavaRDD<Tuple2<Integer,Map<TaggedWord,Double>>> freq_clust  = frequent_nouns(tags_per_cluster_and_total_count, 0.25);
			
			
			// save the frequent nouns object so it can be loaded without being recomputed every time
			//freq_clust.saveAsObjectFile("freq_nouns");
		
			PrintWriter print = new PrintWriter(new FileOutputStream(ClusTweets.storageProps.getProperty("evaluation.frequent.nouns"), false));
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
			//freq_clust = sc.objectFile("freq_nouns");
		
			
			// Only a subset of the most frequent nouns per cluster will appear in the final list of most frequent nouns
			// In this case only the most frequent noun per cluster is kept
			// Arraylist with the most frequent nouns
			ArrayList<Tuple2<TaggedWord,Double>> freq_nouns  = filtered_frequent_nouns(freq_clust);
			
		
			// calculate cluster entropy
			/*
			 * 
			 * Sum -Nci/Nc log(Nci/Nc)
			 * 
			 * where: 
			 * 		Nci = # of tags i inside cluster C
			 * 		Nc = # of tags in cluster C
			 */
			JavaRDD<Tuple2<Integer,Double>> entropy_per_cluster = cluster_entropy(sc, clusters_tagged, freq_nouns);
			// save on disk to see the results
			print = new PrintWriter(new FileOutputStream(ClusTweets.storageProps.getProperty("evaluation.entropy.cluster"), false));
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
			Map<TaggedWord,Double> entropy_per_noun = frequent_nouns_entropy(sc, clusters_tagged, freq_nouns);
		
			// save to disk
			print = new PrintWriter(new FileOutputStream(ClusTweets.storageProps.getProperty("evaluation.entropy.noun"), false));
			print.println("Entropy for each frequent noun\nMinimum value = 0, Maximum value = "+Math.log(150));
			for(TaggedWord w : entropy_per_noun.keySet())
			{
				print.println("Word: "+w.value()+", entropy: "+entropy_per_noun.get(w));
			}
			print.close();
		}catch(Exception e)
		{	
			System.out.println("***Unexpected error!!");
			e.printStackTrace();
		}
	}
	
	
	/**
	 *  This method assigns to each word of a tweet a Part-Of-Speech tag
	 *  
	 *  @param clusters the list of clusters with their tweets
	 *  @return  The list of clusters with their tweets tagged
	 */
	private static JavaRDD<Tuple2<Integer,ArrayList<List<TaggedWord>>>> tag_tweets(JavaPairRDD<Integer,Iterable<String>> clusters)
	{
		// for each cluster tag all the words in its tweet. In other words to each tweet word a tag is assigned
		// indicating whether the word is a noun, adjective, verb, ...
		JavaRDD<Tuple2<Integer,ArrayList<List<TaggedWord>>>> clusters_tagged = clusters.map((tuple) -> {

			
	    	List<List<Word>> sentences = Lemmatizer.lemmatize2(tuple._2);
	    	
			// use the Tagger library to tag all the tweets
			MaxentTagger tagger = new MaxentTagger(ClusTweets.storageProps.getProperty("word.tagger"));
		    ArrayList<List<TaggedWord>> list = new ArrayList();
		    for (List<Word> sentence : sentences) {
		      List<TaggedWord> tSentence = tagger.tagSentence(sentence);
		      //System.out.println(SentenceUtils.listToString(tSentence, false));
		      list.add(tSentence);
		    }
	        
	        return new Tuple2<Integer,ArrayList<List<TaggedWord>>>(tuple._1,list);
	    });
		
		return clusters_tagged;
	}
	
	/**
	 * This method filters the tags of each tweet and keeps only the tags referring to a noun
	 * 
	 * @param clusters_tagged The list of clusters with all their tweets tagged
	 * @return The list of clusters with only noun tags
	 */
	private static JavaRDD<Tuple2<Integer,Tuple2<Map<TaggedWord,Integer>,Integer>>> filter_nouns(JavaRDD<Tuple2<Integer,ArrayList<List<TaggedWord>>>> clusters_tagged)
	{
		// For each cluster keep only the words tagged as nouns as the are supposed to carry most of the information in the tweet
		// Integer is the cluster number
		// TaggedWord is a word tagged as noun
		// Double is the number of occurences of that word in the current cluster
		// Integer is the total number of tags in this cluster
		JavaRDD<Tuple2<Integer,Tuple2<Map<TaggedWord,Integer>,Integer>>> tags_per_cluster_and_total_count = clusters_tagged.map((tuple) -> {
			
			ArrayList<List<TaggedWord>> list = tuple._2();
			
			Map<TaggedWord,Integer> freq = new HashMap();
			for (int i = 0; i < list.size(); i++)
			{
				Iterator<TaggedWord> it = list.get(i).iterator();
				while(it.hasNext())
				{
					TaggedWord w = it.next();
					// list of tags indicating nouns either proper, plural or singular
					String[] tags = {"NN","NNS","NNP","NNPS"};
					for (int j = 0; j < tags.length; j++)
					{
						if (w.tag().compareTo(tags[j]) == 0)
						{
							if (!freq.containsKey(w))
							{
								freq.put(w,1);
							}
							else
							{
								int v = freq.get(w);
								freq.put(w, v+1);
							}
						}
					}
				}
			}
			
			return new Tuple2<Integer,Tuple2<Map<TaggedWord,Integer>,Integer>>(tuple._1(),new Tuple2(freq,list.size()));
		});
		
		return tags_per_cluster_and_total_count;
	}
	
	/**
	 * 
	 * This method calculates the frequuent nouns inside each cluster
	 * 
	 * @param tags_per_cluster_and_total_count The list of tags in each cluster and the total number of occurences 
	 * of that tag
	 * @param threshold The minimum frequency of a tag in order to consider it frequent
	 * @return The list of clusters with their most frequent tags
	 */
	private static JavaRDD<Tuple2<Integer,Map<TaggedWord,Double>>> frequent_nouns(JavaRDD<Tuple2<Integer,Tuple2<Map<TaggedWord,Integer>,Integer>>> tags_per_cluster_and_total_count, double threshold)
	{
		// Find the most frequent nouns inside each cluster.
		// Integer is the cluster number
		// TaggedWord is the tagged word
		// Double is the frequency of the tagged word in the Integer cluster
		JavaRDD<Tuple2<Integer,Map<TaggedWord,Double>>> freq_clust  = tags_per_cluster_and_total_count.map((tuple) -> {
			
			Map<TaggedWord,Integer> freq = tuple._2()._1();
			int size = tuple._2()._2();
			
			Iterator<Map.Entry<TaggedWord,Integer>> it = freq.entrySet().iterator();
			Map<TaggedWord,Double> out = new HashMap();
			while(it.hasNext())
			{
				Map.Entry<TaggedWord,Integer> w = it.next();
				
				double v = w.getValue()/(double)size;
				if (v >= threshold)
					out.put(w.getKey(), v);
			}
			
			return new Tuple2<Integer,Map<TaggedWord,Double>>(tuple._1(),out);
		});
		
		return freq_clust;
		
	}
	
	/**
	 * This method saves for each cluster only the most frequent noun. Each of this nouns is returned as output
	 * 
	 * @param freq_clust The list of clusters with their most frequent tags
	 * @return The list containing the single most frequent tag for each cluster
	 */
	private static ArrayList<Tuple2<TaggedWord,Double>> filtered_frequent_nouns(JavaRDD<Tuple2<Integer,Map<TaggedWord,Double>>> freq_clust)
	{
		// Only a subset of the most frequent nouns per cluster will appear in the final list of most frequent nouns
		// In this case only the most frequent noun per cluster is kept
		JavaPairRDD<Integer,ArrayList<Tuple2<TaggedWord,Double>>> top_nouns_per_cluster = freq_clust.mapToPair((tuple) -> {
			
			Map<TaggedWord,Double> freq_nouns = tuple._2;
			
			ArrayList<Tuple2<TaggedWord,Double>> most_freq_nouns = new ArrayList();
			TaggedWord one = null;
			for (TaggedWord w : freq_nouns.keySet())
			{
				if (one == null)
				{
					one = w;
				}
				else if (freq_nouns.get(w) > freq_nouns.get(one))
				{
					one = w;
				}
			}
			if (one != null)
				most_freq_nouns.add(new Tuple2(one,freq_nouns.get(one)));
			
			return new Tuple2(0, most_freq_nouns);
		});
	
		JavaPairRDD<Integer,ArrayList<Tuple2<TaggedWord,Double>>>  tmp = top_nouns_per_cluster.reduceByKey((tuple1,tuple2)->{
			
			ArrayList<Tuple2<TaggedWord,Double>> a1 = tuple1;
			ArrayList<Tuple2<TaggedWord,Double>> a2 = tuple2;
			
			ArrayList<Tuple2<TaggedWord,Double>> tot = new ArrayList();
			if (a1 != null) {
				tot.addAll(a1);
			}
			if (a2 != null) {
				tot.addAll(a2);
			}
			
			return tot;
		});
		
		// Arraylist with the most frequent nouns
		ArrayList<Tuple2<TaggedWord,Double>> freq_nouns = tmp.first()._2();
		
		return freq_nouns;
	}
	
	
	/**
	 * This method calculates the entropy for each cluster
	 * 
	 * @param sc Java Spark Context
	 * @param clusters_tagged The list of clusters with all of their tags
	 * @param freq_nouns The list with the most frequent nouns, one for each cluster
	 * @return A list of clusters with their corresponding entropy
	 */
	private static JavaRDD<Tuple2<Integer,Double>>  cluster_entropy(JavaSparkContext sc, JavaRDD<Tuple2<Integer,ArrayList<List<TaggedWord>>>> clusters_tagged,ArrayList<Tuple2<TaggedWord,Double>> freq_nouns)
	{
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
		final Broadcast<ArrayList<Tuple2<TaggedWord,Double>>> freq_nouns_br = sc.broadcast(freq_nouns);
	 
		JavaRDD<Tuple2<Integer,Double>> entropy_per_cluster = clusters_tagged.map((tuple) ->{
			
			ArrayList<Tuple2<TaggedWord,Double>> freq_nouns_total = freq_nouns_br.getValue();
			
			ArrayList<List<TaggedWord>> tags_in_current_cluster = tuple._2();
			
			Map<TaggedWord,Integer> noun_tags = new HashMap();
			// filter tags in order to keep only nouns
			int total = 0;
			for (int i = 0; i < tags_in_current_cluster.size(); i++)
			{
				Iterator<TaggedWord> it = tags_in_current_cluster.get(i).iterator();
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
								// the old value is overwritten because keys are unique
								noun_tags.put(w, v+1);
							}
							total++;
						}
					}
				}
			}
			
			//System.out.println("TOTAL is: "+total+", cluster #: "+tuple._1());
			
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
					{
						entropy += (o/total) * Math.log(o/total)*(-1);
						//System.out.println("TOTAL is: "+total+", cluster #: "+tuple._1()+", occur: "+o);
					}
					//System.out.println(entropy+" occ: "+o+", total: "+total);
				}
			}
			
			return new Tuple2<Integer,Double>(tuple._1(),entropy);
		});
		
		return entropy_per_cluster;
	}
	
	
	/**
	 *  This method calculates the entropy of each frequent noun
	 * @param sc Java Spark Context
	 * @param clusters_tagged The list of clusters with all of their tags
	 * @param freq_nouns The list with the most frequent nouns, one for each cluster
	 * @return The list of frequent nouns with their entropy
	 */
	private static Map<TaggedWord,Double> frequent_nouns_entropy(JavaSparkContext sc, JavaRDD<Tuple2<Integer,ArrayList<List<TaggedWord>>>> clusters_tagged,ArrayList<Tuple2<TaggedWord,Double>> freq_nouns)
	{
		// calculate noun entropy
		/*
		 * 
		 * Sum -Nci/Ni log(Nci/Ni)
		 * 
		 * where: 
		 * 		Nci = # of tags i inside cluster C
		 * 		Ni = # of tags i
		 */
		
				
		Tuple2<Integer,Tuple2<Map<TaggedWord,Integer>,Integer>> tmp = filter_nouns(clusters_tagged).mapToPair((tuple) ->{
				return new Tuple2<Integer,Tuple2<Map<TaggedWord,Integer>,Integer>>(0,tuple._2());
			}).reduceByKey((tuple1,tuple2) ->{
				
				Map<TaggedWord,Integer> a1 = tuple1._1;
				Map<TaggedWord,Integer> a2 = tuple2._1;
				
				Map<TaggedWord,Integer> tot = new HashMap();
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
				
				return new Tuple2(tot,0);
				
			}).first();
			
		// map with word tagged and total count of occurences
		Map<TaggedWord,Integer> tags_and_total_count= tmp._2._1;
		
		//Distribute frequent nouns in broadcast to the workers
		Broadcast<Map<TaggedWord,Integer>> tags_tot_count = sc.broadcast(tags_and_total_count);
		Broadcast<ArrayList<Tuple2<TaggedWord,Double>>> freq_nouns_br = sc.broadcast(freq_nouns);
		 
		System.out.println("size: "+tags_and_total_count.size());
		
		Map<TaggedWord,Double> entropy_per_noun = filter_nouns(clusters_tagged).mapToPair((tuple) ->
		{
			ArrayList<Tuple2<TaggedWord,Double>> freq_nouns_total = freq_nouns_br.getValue();
			Map<TaggedWord,Integer> tags_tot_count2 = tags_tot_count.value();
			
			
			Map<TaggedWord,Integer> noun_tags = tuple._2()._1();
			
			
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
			
		}).reduceByKey((tuple1,tuple2)->{
			
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
				
		
		
		return entropy_per_noun;
	}
}
