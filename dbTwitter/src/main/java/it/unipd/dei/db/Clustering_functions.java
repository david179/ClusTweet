package it.unipd.dei.db;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.feature.Word2VecModel;
import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import it.unipd.dei.db.Utils.Distance;
import it.unipd.dei.db.Twitter;

import scala.Tuple2;

public class Clustering_functions {
	
  /* 
   * Distribute the model in broadcast to the workers and calculate for each document the corresponding vector
   * 
   * @version 0.1
   * 
   * @param JavaSparkContext sc, Word2VecModel model,  
   * @param	JavaPairRDD<Twitter, ArrayList<String>> documents, int vector space dimension
   * 
   * @returns JavaRDD<Tuple2<Twitter,Vector>>
  */
  static JavaPairRDD<Twitter,Vector> documentToVector(JavaSparkContext sc, Word2VecModel model,JavaPairRDD<Twitter, ArrayList<String>> documents, int dim) {
	  
		Broadcast<Word2VecModel> bStopWords = sc.broadcast(model);
		
		return documents.mapToPair((doc) -> {
			
			Word2VecModel sw = bStopWords.getValue();
			Iterator<String> it = doc._2().iterator();
			Vector doc_v = Vectors.zeros(dim);
			
			Vector wordVec;
			int count = 0;

			while (it.hasNext())
			{
				/* When a word is not in the vocabulary because it is infrequent, the transform method throws an exception.
				 * However it is not necessary to compute also the infrequent words.
				 */
				try
				{
					wordVec = sw.transform((String)it.next());
					BLAS.axpy(1, wordVec, doc_v);
				}
				catch (Exception e)
				{
					count--;
				}
			
				count++;
			}
			BLAS.scal(count, doc_v);

			return new Tuple2<Twitter,Vector>(doc._1(),doc_v);
		});
  }

  /* 
   * Compute the Farthest-First Traversal on the inputSubset = subset of elements for which we have to calculate k_param centers
   * 
   * @version 0.1
   * 
   * @param Tuple2<Integer, ArrayList<Tuple2<Twitter, Vector>>> inputSubset, int number of centers to find
   * @returns Tuple2<Integer, ArrayList<Tuple2<Twitter, Vector>>> k_param centers of the inputSubset
  */
  static Tuple2<Integer, ArrayList<Tuple2<Twitter, Vector>>> farthestFirstTraversal(Tuple2<Integer, ArrayList<Tuple2<Twitter, Vector>>> inputSubset, 
		  																			 int k_param) {
	   
	   Integer subsetID = inputSubset._1(); 
	   ArrayList<Tuple2<Twitter, Vector>> subsetElements = inputSubset._2(); 

	   ArrayList<Tuple2<Twitter,Vector>> centers = new ArrayList<Tuple2<Twitter,Vector>>();
	   
	   //Number of points of the subset Pj
       int numberOfTuples = subsetElements.size(); 
	    
	   //Number of clusters for each subset Pj 
	   final int K = k_param;
	   
	   //Select a random element as the first center and remove it from the subsetElements
	   int index = randVal(0, numberOfTuples-1); 
	   Tuple2<Twitter,Vector> center = subsetElements.get(index);  
	   centers.add(center); 
	   subsetElements.remove(index);
	      
	   while(centers.size() != K) 
	   {
	       ArrayList<Tuple2<Tuple2<Twitter, Vector>, Double>> finalDistances = new ArrayList<Tuple2<Tuple2<Twitter, Vector>, Double>>(); 
	       
	       //for every point the distance from the set of centers
	       subsetElements.forEach((point) ->{
	    	   double minDistance = Distance.cosineDistance(point._2(), centers.get(0)._2());

	    	   for(int r=1; r<centers.size(); r++)
	    	   {
	    		   double dist = Distance.cosineDistance(point._2(), centers.get(r)._2()); 
	    		   if(dist < minDistance) 
	    		   {
	    			   minDistance = dist; 
	    		   }
	    	   } 
	    	   finalDistances.add(new Tuple2<Tuple2<Twitter, Vector>, Double>(point, minDistance)); 
	       }); 

	       //Select the maximum distance from finalDistances 
	       Tuple2<Tuple2<Twitter, Vector>, Double> newTuple = finalDistances.get(0); 
	       Tuple2<Tuple2<Twitter, Vector>, Double> max = newTuple; 

	       for(int r=1; r<finalDistances.size(); r++)
	       {
	    	   if(finalDistances.get(r)._2() > max._2())
	    	   {
	    		   max = finalDistances.get(r); 
	    	   }
	       }

	       centers.add(max._1()); 
	       subsetElements.remove(max._1()); 
	   } 

	   return new Tuple2<Integer, ArrayList<Tuple2<Twitter, Vector>>>(subsetID, centers); 
  }
  
  /* 
   * @version 0.1
   * 
   * @param int minVal, int maxVal
   * @returns int between minVal and maxVal 
  */
  static int randVal(int minVal, int maxVal)
  { 
      int diff = maxVal-minVal;
      return (int)(Math.random()*((double)diff+1.0d))+minVal;
  }

  /*
   *  Assign documents to their closest center
   *  @version 0.1
   *  
   *  @param Tuple2<Integer, ArrayList<Tuple2<Twitter, Vector>>> pJ, ArrayList<Tuple2<Integer, Tuple2<Twitter, Vector>>> s
   *  @returns ArrayList<Tuple2<Integer, Tuple2<Twitter, Vector>>>: <cluster number, document>
   */
  static ArrayList<Tuple2<Integer, Tuple2<Twitter, Vector>>> partition(Tuple2<Integer, ArrayList<Tuple2<Twitter, Vector>>> pJ,
		  ArrayList<Tuple2<Integer, Tuple2<Twitter, Vector>>> s) 
  {

	  ArrayList<Tuple2<Integer, Tuple2<Twitter, Vector>>> clusterIndexPlusElement = new ArrayList<Tuple2<Integer, Tuple2<Twitter, Vector>>>(); 

	  pJ._2().forEach((tuple) -> {
	       ArrayList<Tuple2<Integer, Double>> distances = new ArrayList<Tuple2<Integer, Double>>(); 
	       s.forEach((elem) ->{
	                double dist = Distance.cosineDistance(elem._2()._2() , tuple._2());
	                distances.add(new Tuple2<Integer, Double>(elem._1(), dist)); 	            
	       });
	       
	       //Search for the minimum distance of the array distances 
	       Tuple2<Integer, Double> min = distances.get(0); 
	       
	       for(int g=1; g<distances.size(); g++)
	       {
	         if(distances.get(g)._2().doubleValue() < min._2().doubleValue())
	         min = distances.get(g); 
	       }
	       // min._1() = cluster index
	       // tuple = elements of this cluster 
	        
	       clusterIndexPlusElement.add(new Tuple2<Integer, Tuple2<Twitter, Vector>>(min._1(), tuple) );
	  });
	  
	 return clusterIndexPlusElement; 
  }
 
  /*
   * Function to calculate the objective function
   * 
   * @version 0.0
   * 
   * @param JavaPairRDD<Integer, Iterable<Tuple2<Twitter, Vector>>> cluster, ArrayList<Tuple2<Integer, Tuple2<Twitter, Vector>>> centers
   * @returns Double obj_funct
   */
  static Double objectiveFunction (JavaPairRDD<Integer, Iterable<Tuple2<Twitter, Vector>>> cluster,
		  ArrayList< Tuple2 <Integer, Tuple2<Twitter, Vector>>> centers,
		  JavaSparkContext sc){
	  
	  //Distribute centers in broadcast to the workers
	  Broadcast<ArrayList<Tuple2<Integer, Tuple2<Twitter, Vector>>>> arrayCenter = sc.broadcast(centers);
 
	  //Computing maximum distances inside clusters 
	  JavaRDD<Tuple2<Integer, Double>> distances = cluster.map((elem)->{
		  double maxDistCluster = 0;
		  int clust = elem._1;
		  Vector currentCenter = null;
	  
		  //Looking for the corresponding center
		  for(Tuple2<Integer, Tuple2<Twitter, Vector>> t : arrayCenter.value()){
			  if (clust == t._1){
				  currentCenter = t._2._2;
				  break;
			  }  
		  }
	  
		  //Calculating distances with current Center
		  for(Tuple2<Twitter, Vector> t : elem._2){
			  double tmp = Distance.cosineDistance(currentCenter, t._2);
			  if (tmp > maxDistCluster)
				  maxDistCluster = tmp;
		  }
		  return new Tuple2<Integer, Double>(clust, maxDistCluster);
	  	});

	  	return distances.reduce((elem1, elem2)->{
	  		if (elem1._2.compareTo(elem2._2) > 0)
	  			return elem1;
	  		return elem2;
	  	})._2;
  }
}
