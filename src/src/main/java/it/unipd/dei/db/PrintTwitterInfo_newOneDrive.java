package it.unipd.dei.db;
// PRINT MAIN INFO OF A TWITTER ACCOUNT 
import java.util.List;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import it.unipd.dei.db.Twitter;
import it.unipd.dei.db.Clustering_functions;

//import scala.collection.immutable.List;

public class PrintTwitterInfo_newOneDrive { 

   private static final String DRIVER = "org.postgresql.Driver";
   private static final String DATABASE = "jdbc:postgresql://localhost/"; 
   private static final String USER = "postgres"; 
   private static final String PASSWORD = "pass"; 

  
   private static final String SQLQUERY = ( "SELECT * FROM tweet_localusa2;"); 
   
   private static Connection newCon = null; 
   private static Statement newStm = null; 
  	
   
   public static ArrayList<Twitter> getTweets()
   {
	    Connection newCon = null; 
	   	Statement newStm = null; 
	   	ResultSet newRs = null; 

	   	long timeStart; 
	   	long timeEnd; 

	    //Twitter[] twitterList = new Twitter[50000]; 
	   	ArrayList<Twitter> twitterList = new ArrayList<Twitter>();
	   	int counter = 0; 
	    
	    //Data structure to contain data taken from the result set 
	  
	   
    		String tweet_ID = null ; 
    		String dateT = null ; 
    		String hour = null ; 
     		String username = null ; 
     		String nickname = null ; 
     		String biography = null ; 
     		String tweet_content = null ; 
     		String favs = null ; 
     		String rts = null ; 
     		String latitude=null;
     		String longitude = null ; 
     		String country = null; 
     		String place = null; 
     		String profile_picture = null ; 
     		int followers = 0; 
     		int following = 0; 
     		int listed = 0;  
     		String language = null; 
     		String url = null ; 

	    

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

	    timeStart = System.currentTimeMillis(); 
	 		//Create a new connection to the Database 
	 		newCon = DriverManager.getConnection(DATABASE, USER, PASSWORD); 
	 		timeEnd = System.currentTimeMillis(); 

	    System.out.println("Connection to the database established in : " +  String.valueOf(timeEnd- timeStart) + "ms") ; 
	   
	    timeStart = System.currentTimeMillis(); 
	 		// Create a new instruction to execute 
	 		newStm = newCon.createStatement(); 
	    timeEnd = System.currentTimeMillis(); 
	    System.out.println("New statement created in : " +  String.valueOf(timeEnd- timeStart) + "ms") ;

	 		timeStart = System.currentTimeMillis(); 
	 		//Create a new Result Set 
	 		newRs = newStm.executeQuery(SQLQUERY); 
	    timeEnd = System.currentTimeMillis(); 
	    System.out.println("Query executed in : " +  String.valueOf(timeEnd- timeStart) + "ms") ;


	 		//Get data from the Result Set 
	    
	 		while(newRs.next() )
	 		{
	 		   tweet_ID = newRs.getString("tweet_ID"); 
	 		   dateT = newRs.getString("datetweet");
                           hour = newRs.getString("hour");
                           username = newRs.getString("username");
                           nickname = newRs.getString("nickname");
                           biography = newRs.getString("biography");
                           tweet_content = newRs.getString("tweet_content");
                           favs = newRs.getString("favs");
                           rts = newRs.getString("rts");
                           latitude = newRs.getString("latitude");
                           longitude = newRs.getString("longitude");
                           country = newRs.getString("country");
                           place = newRs.getString("place");
                           profile_picture = newRs.getString("profile_picture");
                           followers = newRs.getInt("followers");
                           following = newRs.getInt("following");
                           listed = newRs.getInt("listed");
                           language = newRs.getString("language");
                           url = newRs.getString("url");
                                                         
	 			
twitterList.add(new Twitter(tweet_ID, dateT, hour, username, nickname, biography, tweet_content, favs, rts, latitude, longitude, country, place, profile_picture, followers, following, listed, language, url) );

	 		}
	 		

	 	}catch(SQLException e){
	 		
	 		//System.out.println("Database access error!"); 

	    while(e != null)
	    {
	      System.out.printf("- Message:'%s%n",e.getMessage());
	      System.out.printf("- SQL status code: %s%n",e.getSQLState());
	      System.out.printf("-SQL error code: %s%n",e.getErrorCode());
	      System.out.printf("%n");
	      e = e.getNextException();
	    } 

	 	}finally 
	 	{
	 	 try{
	 			if(newRs != null) 
	 			{   
	 				//Release the result set 
	 				newRs.close(); 
	 			}

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
	 		}catch(SQLException e)
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

	 			newRs = null; 
	 			newStm = null; 
	 			newCon = null; 

	 			System.out.println("The resources have been released to the garbage collector!"); 
	 		}
	 	}
	 	
	 	return twitterList;
	   
   }//[m]getTweets

   public static ArrayList<String> getTexts()
   {
	   Connection newCon = null; 
	   	Statement newStm = null; 
	   	ResultSet newRs = null; 

	   	long timeStart; 
	   	long timeEnd; 

	    //Twitter[] twitterList = new Twitter[50000]; 
	   	ArrayList<Twitter> twitterList = new ArrayList<Twitter>();
                ArrayList<String> twitterTexts = new ArrayList<String>(); 
	   	int counter = 0; 
	    
	    //Data structure to contain data taken from the result set 
	  
	   
    		String tweet_ID = null ; 
    		String dateT = null ; 
    		String hour = null ; 
     		String username = null ; 
     		String nickname = null ; 
     		String biography = null ; 
     		String tweet_content = null ; 
     		String favs = null ; 
     		String rts = null ; 
     		String latitude=null;
     		String longitude = null ; 
     		String country = null; 
     		String place = null; 
     		String profile_picture = null ; 
     		int followers = 0; 
     		int following = 0; 
     		int listed = 0;  
     		String language = null; 
     		String url = null ; 

	    

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

	    timeStart = System.currentTimeMillis(); 
	 		//Create a new connection to the Database 
	 		newCon = DriverManager.getConnection(DATABASE, USER, PASSWORD); 
	 		timeEnd = System.currentTimeMillis(); 

	    System.out.println("Connection to the database established in : " +  String.valueOf(timeEnd- timeStart) + "ms") ; 
	   
	    timeStart = System.currentTimeMillis(); 
	 		// Create a new instruction to execute 
	 		newStm = newCon.createStatement(); 
	    timeEnd = System.currentTimeMillis(); 
	    System.out.println("New statement created in : " +  String.valueOf(timeEnd- timeStart) + "ms") ;

	 		timeStart = System.currentTimeMillis(); 
	 		//Create a new Result Set 
	 		newRs = newStm.executeQuery(SQLQUERY); 
	    timeEnd = System.currentTimeMillis(); 
	    System.out.println("Query executed in : " +  String.valueOf(timeEnd- timeStart) + "ms") ;


	 		//Get data from the Result Set 
	    
	 		while(newRs.next() )
	 		{
	 		   tweet_ID = newRs.getString("tweet_ID"); 
	 		   dateT = newRs.getString("datetweet");
                           hour = newRs.getString("hour");
                           username = newRs.getString("username");
                           nickname = newRs.getString("nickname");
                           biography = newRs.getString("biography");
                           tweet_content = newRs.getString("tweet_content");
                           favs = newRs.getString("favs");
                           rts = newRs.getString("rts");
                           latitude = newRs.getString("latitude");
                           longitude = newRs.getString("longitude");
                           country = newRs.getString("country");
                           place = newRs.getString("place");
                           profile_picture = newRs.getString("profile_picture");
                           followers = newRs.getInt("followers");
                           following = newRs.getInt("following");
                           listed = newRs.getInt("listed");
                           language = newRs.getString("language");
                           url = newRs.getString("url");
                                                         
	 			
//twitterList.add(new Twitter(tweet_ID, dateT, hour, username, nickname, biography, tweet_content, favs, rts, latitude, longitude, country, place, profile_picture, followers, following, listed, language, url) );

                           twitterTexts.add(tweet_content);  


	 		
                           }
	 		

	 	}catch(SQLException e){
	 		
	 		//System.out.println("Database access error!"); 

	    while(e != null)
	    {
	      System.out.printf("- Message:'%s%n",e.getMessage());
	      System.out.printf("- SQL status code: %s%n",e.getSQLState());
	      System.out.printf("-SQL error code: %s%n",e.getErrorCode());
	      System.out.printf("%n");
	      e = e.getNextException();
	    } 

	 	}finally 
	 	{
	 	 try{
	 			if(newRs != null) 
	 			{   
	 				//Release the result set 
	 				newRs.close(); 
	 			}

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
	 		}catch(SQLException e)
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

	 			newRs = null; 
	 			newStm = null; 
	 			newCon = null; 

	 			System.out.println("The resources have been released to the garbage collector!"); 
	 		}
	 	}
	 	
	 	return twitterTexts;
	   
   }//[m]getTweets




  /* public static void main(String[] args) {
   	
   	Connection newCon = null; 
   	Statement newStm = null; 
   	ResultSet newRs = null; 

   	long timeStart; 
   	long timeEnd; 

    Twitter[] twitterList = new Twitter[51000]; 
    String[] text = new String[51000]; 
   	int counter = 0; 
    
    //Data structure to contain data taken from the result set 
    String tweet_id = null; 
    String tweet_data = null;  
    String url = null; 
    String tweet_text = null; 
   
    

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

    timeStart = System.currentTimeMillis(); 
 		//Create a new connection to the Database 
 		newCon = DriverManager.getConnection(DATABASE, USER, PASSWORD); 
 		timeEnd = System.currentTimeMillis(); 

    System.out.println("Connection to the database established in : " +  String.valueOf(timeEnd- timeStart) + "ms") ; 
   
    timeStart = System.currentTimeMillis(); 
 		// Create a new instruction to execute 
 		newStm = newCon.createStatement(); 
    timeEnd = System.currentTimeMillis(); 
    System.out.println("New statement created in : " +  String.valueOf(timeEnd- timeStart) + "ms") ;

 		timeStart = System.currentTimeMillis(); 
 		//Create a new Result Set 
 		newRs = newStm.executeQuery(SQLQUERY); 
    timeEnd = System.currentTimeMillis(); 
    System.out.println("Query executed in : " +  String.valueOf(timeEnd- timeStart) + "ms") ;


 		//Get data from the Result Set 
    
 		while(newRs.next() )
 		{
 		   tweet_id = newRs.getString("tweet_id"); 
 		   tweet_data = newRs.getString("tweet_data"); 
 		   url = newRs.getString("url"); 
 		   tweet_text = newRs.getString("tweet_text"); 
 			

                   twitterList[counter] = new Twitter(tweet_id, tweet_data, url, tweet_text); 
                   
                   text[counter] = twitterList[counter].getText(); 
 			          counter++; 

 		}

 		for(int i = 0; i < counter; i++)
 		{
 		   System.out.println(text[i]);  
 		}
 		

 	}catch(SQLException e){
 		
 		//System.out.println("Database access error!"); 

    while(e != null)
    {
      System.out.printf("- Message:'%s%n",e.getMessage());
      System.out.printf("- SQL status code: %s%n",e.getSQLState());
      System.out.printf("-SQL error code: %s%n",e.getErrorCode());
      System.out.printf("%n");
      e = e.getNextException();
    } 

 	}finally 
 	{
 	 try{
 			if(newRs != null) 
 			{   
 				//Release the result set 
 				newRs.close(); 
 			}

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
 		}catch(SQLException e)
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

 			newRs = null; 
 			newStm = null; 
 			newCon = null; 

 			System.out.println("The resources have been released to the garbage collector!"); 
 		}
 	}

   }//[m]end main */
   
   
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
	 		
	 		//System.out.println("Database access error!"); 

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
   
   public static void insertTweet(int cluster, String tweet_id)
   {
	    	String query = "insert into clusters values("+cluster+",'"+tweet_id+"');";

	   	
	 	try{ 

	 		// Create a new instruction to execute 
	 		newStm.executeUpdate(query); 
	 		

	 	}catch(SQLException e){
	 		
	 		//System.out.println("Database access error!"); 

		    while(e != null)
		    {
		      System.out.printf("- Message:'%s%n",e.getMessage());
		      System.out.printf("- SQL status code: %s%n",e.getSQLState());
		      System.out.printf("-SQL error code: %s%n",e.getErrorCode());
		      System.out.printf("%n");
		      e = e.getNextException();
		    } 

	 	}
	   
   }//[m]getTweets
   
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
   

}//[c]end class 
