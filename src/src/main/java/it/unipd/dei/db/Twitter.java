package it.unipd.dei.db;
// NEW TWITTER CLASS 
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import java.io.Serializable;

public class Twitter implements Serializable
{

    public static Encoder<Twitter> getEncoder() {
    return Encoders.bean(Twitter.class);
  }

    private  String tweet_ID; 
    private  String dateT; 
    private  String hour; 
    private  String username; 
    private  String nickname; 
    private  String biography; 
    private  String tweet_content; 
    private  String favs; 
    private  String rts; 
    private  String latitude , longitude; 
    private  String country, place; 
    private  String profile_picture; 
    private  int followers, following; 
    private  int listed;  
    private  String language, url; 

    // costruttore della classe 
    public Twitter(String tweet_ID,String dateT,String hour,String username,String nickname, String biography, String tweet_content,String favs, String rts, String latitude, String longitude, String country, String place, String prof, int followers, int following, int listed,  String language, String url)
    { 
       this.tweet_ID = tweet_ID; 
       this.dateT = dateT; 
       this.hour = hour; 
       this.username = username; 
       this.nickname = nickname; 
       this.biography = biography; 
       this.tweet_content = tweet_content;
       this.favs = favs; 
       this.rts = rts; 
       this.latitude = latitude; 
       this.longitude = longitude; 
       this.country = country; 
       this.place = place; 
       profile_picture = prof; 
       this.followers = followers; 
       this.following = following; 
       this.listed = listed; 
       this.language = language; 
       this.url = url;   
    }

    
public void printTwitterInfo() 
{
   System.out.println("ID: " + tweet_ID + " , Date : " + dateT + " , URL : " + url + " , country : " + country ); 
}

public String getText()
{
   return tweet_content; 
}


}//{c}Twitter 
