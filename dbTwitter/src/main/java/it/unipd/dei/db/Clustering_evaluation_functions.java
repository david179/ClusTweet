package it.unipd.dei.db;

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

import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import scala.Tuple2;

public class Clustering_evaluation_functions {

	
	public static JavaRDD<Tuple2<Integer,ArrayList<List<TaggedWord>>>> tag_tweets(JavaPairRDD<Integer,Iterable<String>> clusters)
	{
		// for each cluster tag all the words in its tweet. In other words to each tweet word a tag is assigned
		// indicating whether the word is a noun, adjective, verb, ...
		JavaRDD<Tuple2<Integer,ArrayList<List<TaggedWord>>>> clusters_tagged = clusters.map((tuple) -> {
	    	
	    	Iterator<String> it = tuple._2.iterator();
	        
	    	// break the tweet String into word and save each tweet as a List<Word>
	    	// save all the tweets as a List of Lists
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
			
			// use the Tagger library to tag all the tweets
			MaxentTagger tagger = new MaxentTagger("models/english-left3words-distsim.tagger");
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
	
	
	public static JavaRDD<Tuple2<Integer,Tuple2<Map<TaggedWord,Integer>,Integer>>> filter_nouns(JavaRDD<Tuple2<Integer,ArrayList<List<TaggedWord>>>> clusters_tagged)
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
	
	public static JavaRDD<Tuple2<Integer,Map<TaggedWord,Double>>> frequent_nouns(JavaRDD<Tuple2<Integer,Tuple2<Map<TaggedWord,Integer>,Integer>>> tags_per_cluster_and_total_count, double threshold)
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
	
	public static ArrayList<Tuple2<TaggedWord,Double>> filtered_frequent_nouns(JavaRDD<Tuple2<Integer,Map<TaggedWord,Double>>> freq_clust)
	{
		// Only a subset of the most frequent nouns per cluster will apper in the final list of most frequent nouns
		// In this case only the most frequent noun per cluster is kept
		JavaPairRDD<Integer,ArrayList<Tuple2<TaggedWord,Double>>> top_nouns_per_cluster = freq_clust.mapToPair((tuple) -> {
			
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
				/*else if (two == null)
				{
					two = w;
				}*/
				else if (freq_nouns.get(w) > freq_nouns.get(one))
				{
					one = w;
				}
				/*else if (freq_nouns.get(w) > freq_nouns.get(two))
				{
					two = w;
				}*/
				//most_freq_nouns.add(new Tuple2(w,freq_nouns.get(w)));
			}
			if (one != null)
				most_freq_nouns.add(new Tuple2(one,freq_nouns.get(one)));
			//if (two != null)
				//most_freq_nouns.add(new Tuple2(two,freq_nouns.get(two)));
			
			return new Tuple2(0,most_freq_nouns);
		});
		
		

		JavaPairRDD<Integer,ArrayList<Tuple2<TaggedWord,Double>>>  tmp = top_nouns_per_cluster.reduceByKey((tuple1,tuple2)->{
			
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
		
		// Arraylist with the most frequent nouns
		ArrayList<Tuple2<TaggedWord,Double>> freq_nouns = tmp.first()._2();
		
		return freq_nouns;
	}
	
	
	public static JavaRDD<Tuple2<Integer,Double>>  cluster_entropy(JavaSparkContext sc, JavaRDD<Tuple2<Integer,ArrayList<List<TaggedWord>>>> clusters_tagged,ArrayList<Tuple2<TaggedWord,Double>> freq_nouns)
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
	
	
	
	public static Map<TaggedWord,Double> frequent_nouns_entropy(JavaSparkContext sc, JavaRDD<Tuple2<Integer,ArrayList<List<TaggedWord>>>> clusters_tagged,ArrayList<Tuple2<TaggedWord,Double>> freq_nouns)
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
