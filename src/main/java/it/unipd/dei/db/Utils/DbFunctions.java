package it.unipd.dei.db.Utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import it.unipd.dei.db.ClusTweets;
import it.unipd.dei.db.TwitterClustered;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.ling.Word;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;

public class DbFunctions {

	   public static final String DRIVER = "org.postgresql.Driver";
	   private static final String DATABASE = "jdbc:postgresql://localhost:5433/"; 
	   private static final String USER = "postgres"; 
	   private static final String PASSWORD = "123abcz"; 
 
	   private static Connection newCon = null; 
	   private static Statement newStm = null; 
	  	
	  
	   /**
	   * Create a new connection using the static variables driver, database, user and password. 
	   */
	   public static void openConn()
	   {
		   
		   //Registration of the driver that must be used 
		    try{
		    	Class.forName(DRIVER); 
		    	System.out.println("Driver " + DRIVER + " has been correctly registered."); 
		    }catch(ClassNotFoundException e) {
		    	System.out.println("Driver " + DRIVER + " not found." ); 
		        System.out.println("Error: " + e.getMessage()); 
		    	System.exit(-1); 
		    }

		 	
		 	try{ 

		 		newCon = DriverManager.getConnection(DATABASE, USER, PASSWORD);
		 		newStm = newCon.createStatement(); 
		 		
		 	}catch(SQLException e){
		 		
			    while(e != null)
			    {
			      System.out.printf("- Message:'%s%n",e.getMessage());
			      System.out.printf("- SQL status code: %s%n",e.getSQLState());
			      System.out.printf("-SQL error code: %s%n",e.getErrorCode());
			      System.out.printf("%n");
			      e = e.getNextException();
			    } 
			    try{
					if(newCon != null) 
					{
						//Release the connection 
						newCon.close(); 
					}
		 		}catch(SQLException ee)
		 		{
		 			System.out.println("Error during the release of the resources!"); 
		      
				      while(ee != null)
				      {
				        System.out.printf("- Message:'%s%n",ee.getMessage());
				        System.out.printf("- SQL status code: %s%n",ee.getSQLState());
				        System.out.printf("-SQL error code: %s%n",ee.getErrorCode());
				        System.out.printf("%n");
				        ee = ee.getNextException();
				      } 

		 	}
		 }
		   
	   }

	   /**
	   * Insert a new object of the TwitterClustered class into the clusters table. 
	   * @param tweet the new object to insert into the clusters table 
       *
	   */
	   
	   public static void insertTweet(TwitterClustered tweet)
	   {
		   try{
		    	String prepared_query = "INSERT INTO clusters VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";
		    
		    	PreparedStatement newPStm = newCon.prepareStatement(prepared_query);
			   	newPStm.setString(1, tweet.getTweet_ID());
			   	newPStm.setInt(2, tweet.getCluster());
			   	newPStm.setString(3, tweet.getDateTweet());
			   	newPStm.setString(4, tweet.getHour());
			   	newPStm.setString(5, tweet.getUsername());
			   	newPStm.setString(6, tweet.getNickname());
			   	newPStm.setString(7, tweet.getBiography());
			   	newPStm.setString(8, tweet.getTweet_content());
			   	newPStm.setString(9, tweet.getFavs());
			   	newPStm.setString(10, tweet.getRts());
			   	newPStm.setString(11, tweet.getLatitude());
			   	newPStm.setString(12, tweet.getLongitude());
			   	newPStm.setString(13, tweet.getCountry());
			   	newPStm.setString(14, tweet.getPlace());
			   	newPStm.setString(15, tweet.getProfile_picture());
			   	newPStm.setInt(16, tweet.getFollowers());
			   	newPStm.setInt(17, tweet.getFollowing());
			   	newPStm.setInt(18, tweet.getListed());
			   	newPStm.setString(19, tweet.getLanguage());
			   	newPStm.setString(20, tweet.getUrl());
			 
		 		newPStm.execute();

		 	}catch(SQLException e){
		 		
		 	    while(e != null)
			    {
			      System.out.printf("- Message:'%s%n",e.getMessage());
			      System.out.printf("- SQL status code: %s%n",e.getSQLState());
			      System.out.printf("-SQL error code: %s%n",e.getErrorCode());
			      System.out.printf("%n");
			      e = e.getNextException();
			    } 

		 	}
		   
	   }
	   
	   /**
	     * Close the connection releasing all the resources. 
         *
	   */
	   public static void close()
	   {
		   try{

				if(newStm != null)
				{
					//Release the statement 
					newStm.close(); 
				} 

				if(newCon != null) 
				{
					//Release the connection 
					newCon.close(); 
				}
		   }
		   catch(SQLException e)
		   {
	 		      System.out.println("Error during the release of the resources!"); 
		      
			      while(e != null)
			      {
			        System.out.printf("- Message:'%s%n",e.getMessage());
			        System.out.printf("- SQL status code: %s%n",e.getSQLState());
			        System.out.printf("-SQL error code: %s%n",e.getErrorCode());
			        System.out.printf("%n");
			        e = e.getNextException();
			      } 

		 		} finally
		 		{

		 			newStm = null; 
		 			newCon = null; 

		 			System.out.println("The resources have been released to the garbage collector!"); 
		 		}
	   }
	   

	   /**
	     * @param spark new object of the SparkSession class 
	     * @param table name of the database table 
	     * @returns Dataset<Row> dataset generated after the lecture of the database table "table"
	   */
	   public static Dataset<Row> connectionDb(SparkSession spark, String table){
		   //Registration of the driver that must be used 
		    try{
		    	Class.forName(DRIVER); 
		    	System.out.println("Driver " + DRIVER + " has been correctly registered."); 
		    }catch(ClassNotFoundException e) {
		    	System.out.println("Driver " + DRIVER + " not found." ); 
		        System.out.println("Error: " + e.getMessage()); 
		    	System.exit(-1); 
		    }
		    
			Dataset<Row> jdbcDF = spark.read()
					.format("jdbc")
					.option("url", ClusTweets.storageProps.getProperty("database.uri"))
					.option("dbtable", table)
					.option("user", ClusTweets.storageProps.getProperty("database.user"))
					.option("password", ClusTweets.storageProps.getProperty("database.password"))
					.load();
			return jdbcDF;
		}

	}//[c]end class 