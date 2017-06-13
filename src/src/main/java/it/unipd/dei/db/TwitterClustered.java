package it.unipd.dei.db;

import java.io.Serializable;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class TwitterClustered  implements Serializable
{
   
    private String tweet_ID = null; 
    private int tweet_cluster = 0;
    private Vector tweet_vector = null;
    private String tweet_DATA = null; 
    private String tweet_URL = null;  
    private String tweet_text = null; 

    // costruttore della classe
    public TwitterClustered(){
    }
    
    public TwitterClustered(String tweet_ID, String tweet_DATA, String tweet_URL, String tweet_text)
    { 
       this.tweet_ID = tweet_ID; 
       this.tweet_DATA = tweet_DATA;
       this.tweet_URL = tweet_URL;
       this.tweet_text = tweet_text;   
    }

    public void setTweet_ID(String str){
    	tweet_ID = str;
    }
    public void setTweet_cluster(int str){
    	tweet_cluster = str;
    }
    public void setTweet_vector(Vector str){
    	tweet_vector = str;
    }
    public void setTweet_DATA(String str){
    	tweet_DATA = str;
    }
    public void setTweet_URL(String str){
    	tweet_URL = str;
    }
    public void setTweet_text(String str){
    	tweet_text = str;
    }
    
    public String getTweet_ID(){
    	return tweet_ID;
    }
    public int getTweet_cluster(){
    	return tweet_cluster;
    }
    public Vector getTweet_vector(){
    	return tweet_vector;
    }
    public String getTweet_DATA(){
    	return tweet_DATA;
    }
    public String getTweet_URL(){
    	return tweet_URL;
    }
    public String getTweet_text(){
    	return tweet_text;
    }
    
	public void printTwitterInfo() 
	{
	   System.out.println("ID: " + tweet_ID + " , Date : " + tweet_DATA + " , URL : " + tweet_URL + " , text : " + tweet_text); 
	}
	
	public String getText()
	{
	   return tweet_text; 
	}


}//{c}Twitter 