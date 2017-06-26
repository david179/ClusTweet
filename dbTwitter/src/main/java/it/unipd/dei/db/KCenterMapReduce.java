package it.unipd.dei.db; 

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.lang.Iterable; 

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.feature.Word2Vec;
import org.apache.spark.mllib.feature.Word2VecModel;

import it.unipd.dei.db.Utils.Lemmatizer;
import it.unipd.dei.db.Twitter;
import it.unipd.dei.db.Clustering_functions; 

import scala.Tuple2;


/**
 * @brief Map-Reduce implementation of K-center algorithm
 *
 * @authors T.Agnolazza, A.Ciresola, D.Lucchi, F.Paganin 
 * @version 5.0 
 *
 * Round 3 optimized
 * Substitution of Partition(P,S) with Partition(Pj, S) in parallel for all subsets Pj 
 *
 * @date 14/05/2017 
 * last review 03/06/2017 
 */

public class KCenterMapReduce
{
	public static Dataset<TwitterClustered> cluster(Dataset<Twitter> args, SparkSession spark) throws IOException, KcoeffCustomException
	{ 
		//System.setProperty("hadoop.home.dir", "c:\\winutil\\");
	
		//***************************************Preprocessing***************************************
		//Acquisition of the cluster number "k" and path of dataset as parameters
		final int k = 36;
		final int k_coeff = 2;
		//vector space dimension
		final int dim = 100;
	
		System.out.println("Starting clustering routine");
		//Spark setup
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    
		JavaRDD<Twitter> tweets = args.toJavaRDD();
    
		System.out.println("\n***************************************************************************************"); 
		System.out.println("Essential information in the first tweet");
		tweets.first().printTwitterInfo(); 
		System.out.println("Number of tweets: "+ tweets.count() );
		System.out.println("***************************************************************************************"); 
    
		//Getting only the content of a tweet
		JavaRDD<String> texts = tweets.map((p) -> p.getTweet_content());  
 
		System.out.println("\n***************************************************************************************"); 
		System.out.println(texts.first()); 
		System.out.println("***************************************************************************************"); 


		System.out.println("Tweet content mapping finished");        

		//Lemmatization
		JavaRDD<ArrayList<String>> lemmas = Lemmatizer.lemmatize(texts).cache();  

		System.out.println("\n***************************************************************************************"); 
		System.out.println("Debug for lemmatization ");
		ArrayList<String> aa = lemmas.first(); 
		aa.forEach((elem) -> 
		{
			System.out.println(elem); 
		});
		System.out.println("***************************************************************************************");
		
		System.out.println("Lemmatization finished");
    
		//Fit the word2vec model and save it in memory
		Word2VecModel model = null;
		try{
			//Load model from memory
			model = Word2VecModel.load(sc.sc(), "Models/");
		}
		catch(Exception e){
			new Word2Vec()
			.setVectorSize(dim)
			.fit(lemmas).save(sc.sc(), "Models/");

			System.out.println("Model created");
			model = Word2VecModel.load(sc.sc(), "Models/");
		}
    
		JavaPairRDD<Twitter, ArrayList<String>> pagesAndLemma = tweets.zip(lemmas);
    
		//Map documents to vectors
		JavaPairRDD<Twitter,Vector> pagesAndVectors =  Clustering_functions.documentToVector(sc, model, pagesAndLemma, dim);

		System.out.println("\n*******************************************************************************");  
		System.out.println( pagesAndVectors.first()._1().getTweet_content() ); 
		System.out.println("*******************************************************************************");    

		try
		{
			//Set of points P = pagesAndVectors 
		    //n = number of elements in the set 
		    int n = (int)pagesAndVectors.count();  
		
		    //l = number of subsets of pagesAndVectors 
		    double l = Math.sqrt(n/k);
		    
		    //appl = int number of subsets 
		    int appl = (int)Math.floor(l);
	    
		    //*************************************end Preprocessing*************************************
		    
		    //******************************************Round 1******************************************
		    
		    //Map each element <Wi, Vi> into <<Wi, Vi> , i>
		    JavaPairRDD<Tuple2<Twitter,Vector> , Long> pagesAndVectorsWithIndex = pagesAndVectors.zipWithIndex(); 
		
		    //New indexes assigned at each pair with index that points to the bucket index from 0 to sqrt(n/k)-1
		    JavaPairRDD<Integer , Tuple2<Twitter,Vector>> newpagesAndVectors = pagesAndVectorsWithIndex.mapToPair((tuple) -> { 
            		int tmp = (int) (long) (tuple._2()); 
            		int index = (tmp%appl); 
 
            		return new Tuple2< Integer , Tuple2<Twitter, Vector> >(index , tuple._1()); 
        		}
		    ); 

		    //Each element of the following RDD is a subset Pj of the initial set of points P 
		    JavaPairRDD<Integer, Iterable<Tuple2<Twitter, Vector>>> pagesGroupedByKey = newpagesAndVectors.groupByKey(); 
	     
		    /* Now we have to run the Farthest-First Traversal algorithm on each element of the previous RDD
		     * 
		     * JavaPairRDD<Integer, Iterable<Tuple2<Twitter, Vector>>> 
		     * will be converted into 
		     * JavaPairRDD <Integer, ArrayList<Tuple2<Twitter, Vector>>> 
		     * because the last one is easier to manage
		     */ 
		    JavaPairRDD<Integer, ArrayList<Tuple2<Twitter, Vector>>> pagesGroupedByKeyArrayList = pagesGroupedByKey.mapToPair((tuple) ->{ 
	            	ArrayList<Tuple2<Twitter, Vector>> tempArray = new ArrayList<Tuple2<Twitter, Vector>>(); 
	            	Iterator<Tuple2<Twitter, Vector>> newIterator = tuple._2().iterator(); 
	            
	            	while(newIterator.hasNext()) 
	            		tempArray.add(newIterator.next()); 
	            
	            	return new Tuple2<Integer, ArrayList<Tuple2<Twitter, Vector>>>(tuple._1() , tempArray); 
	          	}
		    );
		
		    //We need to check if the value of k' is compatible with the number of elements inside each subsets
		    int elements_per_subset = n/appl;
		    int k_first = (int)(k_coeff*k);
		    if (k_first > elements_per_subset)
		    {	
		    	//We must stop because we were trying to take from each subset a number of centers greater than the number of elements in the subset
		    	throw new KcoeffCustomException("k'= "+k_first+ " is bigger than the number of elements for each subset ("+elements_per_subset+")");	    
		    }
	
		    JavaPairRDD<Integer, ArrayList<Tuple2<Twitter, Vector>>> centersForEachSubset = pagesGroupedByKeyArrayList.mapToPair((tuple) ->{   
	             	//Invoke the Farthest-First Traversal with parameter k'> k, in this specific case k' = 2k 
	             	return Clustering_functions.farthestFirstTraversal(tuple ,k_first); 
	         	}
		    ).cache();
		    
		    //****************************************end Round 1****************************************

		    //******************************************Round 2******************************************
	    
		    JavaPairRDD<Integer, ArrayList<Tuple2<Twitter,Vector>>> tuplesToJoin = centersForEachSubset.mapToPair((tuple) ->{
		    	return new Tuple2<Integer, ArrayList<Tuple2<Twitter, Vector>>>(0, tuple._2()); 
		    });

		    JavaPairRDD<Integer, ArrayList<Tuple2<Twitter,Vector>>> tuplesToJoin2 = tuplesToJoin.reduceByKey((value1, value2) -> {
	           	//Union of two ArrayList 
	            value2.forEach((elem) -> {
	                value1.add(elem); 
	            });
	
	            return value1; 
		    }).cache();
	
		    //TuplesToJoin2 contains only one tuple (k,V) [K=0, value = all centers]

		    Tuple2<Integer, ArrayList<Tuple2<Twitter, Vector>>> tuplesToJoin3 = tuplesToJoin2.first(); 
	    
		    //Farthest-First Traversal to find k centers 
		    Tuple2<Integer, ArrayList<Tuple2<Twitter, Vector>>> finalCenters = Clustering_functions.farthestFirstTraversal(tuplesToJoin3, k); 
	
		    //extractedCenters = S = {c1, c2, ..., ck} = k centers 
		    ArrayList<Tuple2<Twitter, Vector>> extractedCenters = finalCenters._2();
		    //****************************************end Round 2****************************************
		
		    //******************************************Round 3******************************************	    
		    ArrayList<Tuple2<Integer, Tuple2<Twitter, Vector>>> clusters = new ArrayList<Tuple2<Integer, Tuple2<Twitter, Vector>>>(); 
		
		    for(int j=0; j<extractedCenters.size(); j++) 
		    	clusters.add(new Tuple2<Integer, Tuple2<Twitter, Vector>>(Integer.valueOf(j), extractedCenters.get(j)) );   
		
		    //clusters contains all pairs (0, c0), (1,c1),...,(k-1, c(k-1))
		    //pagesGroupedByKeyArrayList = set of all subsets Pj

		    //Now invoke the partition method on all subsets Pj
		    JavaRDD<ArrayList<Tuple2<Integer, Tuple2<Twitter, Vector>>>> clustersRound3 = pagesGroupedByKeyArrayList.map((tuple) -> {
			     return Clustering_functions.partition(tuple, clusters);
			}); 
 
		    JavaPairRDD<Integer,  ArrayList<Tuple2<Integer, Tuple2<Twitter, Vector>>>> fix = clustersRound3.mapToPair((tuple) -> { 
		    	return new Tuple2<Integer, ArrayList<Tuple2<Integer, Tuple2<Twitter, Vector>>>> (Integer.valueOf(0), tuple);
		    });
		
		    JavaPairRDD<Integer, ArrayList<Tuple2<Integer, Tuple2<Twitter, Vector>>>> finalFix = fix.reduceByKey((value1, value2) -> {
		    	value2.forEach((elem) -> {
		    		value1.add(elem); 
		    	});
		    	return value1; 
		    }).cache();
		
		    //In finalFix there's only one element
		    JavaPairRDD<Integer, Tuple2<Twitter, Vector>> newRDD = sc.parallelizePairs(finalFix.first()._2()); 
		    
		    
		    JavaRDD<TwitterClustered> RDDTwitter = newRDD.map((elem) -> {
		    	
		    	TwitterClustered tmp = new TwitterClustered(elem._2._1.getTweet_ID(), elem._2._1.getDateTweet(), 
		    			elem._2._1.getHour(), elem._2._1.getUsername(), elem._2._1.getNickname(), 
		    			elem._2._1.getBiography(), elem._2._1.getTweet_content(), elem._2._1.getFavs(), 
		    			elem._2._1.getRts(), elem._2._1.getLatitude(), elem._2._1.getLongitude(), 
		    			elem._2._1.getCountry(), elem._2._1.getPlace(), elem._2._1.getProfile_picture(), elem._2._1.getFollowers(),
		    			elem._2._1.getFollowing(), 
		    			elem._2._1.getListed(), 
		    			elem._2._1.getLanguage(), elem._2._1.getUrl());
		    	
		    	tmp.setCluster(elem._1);
			
		    	return tmp;
		    });
		
		    JavaPairRDD<Integer, Iterable<Tuple2<Twitter, Vector>>> groupedFinalClusters = newRDD.groupByKey(); 		  
		
		    
		    
		    Dataset<Row> tweetClusteredRow = spark.createDataFrame(RDDTwitter.rdd(), TwitterClustered.class);
		
		    
		    
		    Encoder<TwitterClustered> twitterEncoder = Encoders.bean(TwitterClustered.class);
		    
		    Dataset<TwitterClustered> tweetClustered = tweetClusteredRow.as(twitterEncoder);
		 		  
		    //tweetClustered.collect();
		    //****************************************end Round 3****************************************
		
		    //Calculating the objective function
		    double f_obj = Clustering_functions.objectiveFunction(groupedFinalClusters, clusters, sc);
	    
		    //*************************************Diagnostic strings************************************
		    System.out.println("");
		    for(int f=0; f<k; f++)
		    {
		        List<Iterable<Tuple2<Twitter, Vector>>> alist = groupedFinalClusters.lookup(Integer.valueOf(f));
		   
		        System.out.println("***********************************************************************************************"); 
		        System.out.println(); 
		        System.out.println("Number of cluster:" + f); 
		        System.out.println(); 
	
		        alist.forEach((elem1) -> {
		           elem1.forEach((elem) -> {
		        	   System.out.println("Twitter text : " + elem._1().getTweet_content());
		           });
		        });
		    } 

		    System.out.println("***********************************************************************************************"); 
		    System.out.println("***********************************************************************************************"); 
		    System.out.println(); 
	
		    System.out.println("Objective function value: "); 
		    System.out.println("[" + f_obj + "]");
		    System.out.println();
		    
		    return tweetClustered;
		}
		catch(Exception e){
			System.out.println("A fatal error occured");
			e.printStackTrace();
		}
		return null;
	}
}
