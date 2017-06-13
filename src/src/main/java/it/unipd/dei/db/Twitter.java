package it.unipd.dei.db;

import java.io.Serializable;

public class Twitter  implements Serializable
{
   
    private String tweet_ID = null; 
    private String tweet_DATA = null; 
    private String tweet_URL = null; 
    private String tweet_text = null; 

    // costruttore della classe
    public Twitter(){
    }
    
    public Twitter(String tweet_ID, String tweet_DATA, String tweet_url, String tweet_text)
    { 
       this.tweet_ID = tweet_ID; 
       this.tweet_DATA = tweet_DATA;
       this.tweet_URL = tweet_url;
       this.tweet_text = tweet_text;   
    }

    public void setTweet_ID(String str){
    	tweet_ID = str;
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
