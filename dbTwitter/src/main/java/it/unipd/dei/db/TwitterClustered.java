package it.unipd.dei.db;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import java.io.Serializable;

public class TwitterClustered implements Serializable
{
    private  String tweet_ID; 
    private  String dateTweet; 
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
    private  Integer followers, following; 
    private  Integer listed;  
    private  String language, url; 
    private int cluster;

    public TwitterClustered(){
    	super();
    }
    

    public TwitterClustered(String tweet_ID,String dateT,String hour,String username,String nickname, String biography, String tweet_content,String favs, String rts, String latitude, String longitude, String country, String place, String prof, int followers, int following, int listed,  String language, String url)
    { 
       this.tweet_ID = tweet_ID; 
       this.dateTweet = dateT; 
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
       this.cluster = 0;
    }
    
    /**
    * Print main twitter info 
    */
	public void printTwitterInfo() 
	{
	   System.out.println("ID: " + tweet_ID + " , Date : " + dateTweet + " , URL : " + url + " , country : " + country ); 
	}
    
    /**
    * Set the tweet_ID param 
    * @param tmp tweet_ID to set 
    */
    public void setTweet_ID(String tmp){
    	tweet_ID = tmp;
    }
    
    /**
    * @return the tweet_ID param 
    */
    public String getTweet_ID(){
    	return tweet_ID;
    }
    
    /**
    * Set the date associated to a specific tweet 
    * @param tmp date of the tweet to set 
    */
    public void setDateTweet(String tmp){
    	dateTweet = tmp;
    }
    
    /**
    * @return the date of the tweet 
    */
    public String getDateTweet(){
    	return dateTweet;
    }
    
    /**
    * Set the hour associated to the release of a tweet 
    * @param tmp tweet release time 
    */
    public void setHour(String tmp){
    	hour = tmp;
    }
    
    /**
    * @return the release hour of the tweet 
    */
    public String getHour(){
    	return hour;
    }
    
    /**
    * Set the username associated to the user that has released the tweet  
    * @param tmp username to insert 
    */
    public void setUsername(String tmp){
    	username = tmp;
    }
    
    /**
    * @return the username associated to a specific tweet 
    */
    public String getUsername(){
    	return username;
    }
    
    /**
    * Set the nickname associated to the user that has released the tweet 
    * @param tmp nickname to insert 
    */
    public void setNickname(String tmp){
    	nickname = tmp;
    }
    
    /**
    * @return the nickname associated to the tweet 
    */
    public String getNickname(){
    	return nickname;
    }
    
    /**
    * Set the biography of the user that has released the tweet  
    * @param tmp biography of the user 
    */
    public void setBiography(String tmp){
    	biography = tmp;
    }
    
    /**
    * @return the user biography associated to the tweet 
    */
    public String getBiography(){
    	return biography;
    }
    
    /**
    * Set the tweet content 
    * @param tmp tweet content to insert 
    */
    public void setTweet_content(String tmp){
    	tweet_content = tmp;
    }
    
    /**
    * @return the content of the tweet 
    */
    public String getTweet_content(){
    	return tweet_content;
    }
    
    /**
    * Set favorites associated to the tweet 
    * @param tmp favorites to add to the tweet 
    */
    public void setFavs(String tmp){
    	favs = tmp;
    }
    
    /**
    * @return favorites associated to the tweet 
    */
    public String getFavs(){
    	return favs;
    }
    
    /**
    * Set the retweets associated to a tweet 
    * @param tmp retweets to set 
    */
    public void setRts(String tmp){
    	rts = tmp;
    }
    
    /**
    * @return the retweets associated to the tweet 
    */
    public String getRts(){
    	return rts;
    }
    
    /**
    * Set latitude of the place from which the tweet has been released 
    * @param tmp latitude to set 
    */
    public void setLatitude(String tmp){
    	latitude = tmp;
    }
    
    /**
    * @return latitude associated to the tweet 
    */
    public String getLatitude(){
    	return latitude;
    }
    
    /**
    * Set longitude of the place from which the tweet has been released 
    * @param tmp longitude to set 
    */
    public void setLongitude(String tmp){
    	longitude = tmp;
    }
    
    /**
    * @return longitude associated to the tweet 
    */
    public String getLongitude(){
    	return longitude;
    }
    
    /**
    * Set the country from which the tweet has been released 
    * @param tmp country to set 
    */
    public void setCountry(String tmp){
    	country = tmp;
    }
    
    /**
    * @return the country associated to the tweet 
    */
    public String getCountry(){
    	return country;
    }
    
    /**
    * Set the place from which the tweet has been released 
    * @param tmp place of the release 
    */
    public void setPlace(String tmp){
    	place = tmp;
    }
    
    /**
    * @return the place associated to the tweet 
    */
    public String getPlace(){
    	return place;
    }
    
    /**
    * Set the profile picture of the user who has released the tweet 
    * @param tmp link to the profile picture 
    */
    public void setProfile_picture(String tmp){
    	profile_picture = tmp;
    }
    
    /**
    * @return the link of the profile picture of the user associated to the tweet 
    */
    public String getProfile_picture(){
    	return profile_picture;
    }
    
    /**
    * Set the number of followers of the user who has released the tweet 
    * @param tmp number of followers 
    */
    public void setFollowers(Integer tmp){
    	followers = tmp;
    }
    
    /**
    * @return the number of followers associated to the tweet 
    */
    public Integer getFollowers(){
    	return followers;
    }
    
    /**
    * Set the number of following associated to the tweet 
    * @param tmp number of following to set 
    */
    public void setFollowing(Integer tmp){
    	following = tmp;
    }
    
    /**
    * @return the number of following associated to the tweet 
    */
    public Integer getFollowing(){
    	return following;
    }
    
    /**
    * Set the number of listed 
    * @param tmp number of listed to set 
    */
    public void setListed(Integer tmp){
    	listed = tmp;
    }
    
    /**
    * @return the number of listed associated to the user that has released the tweet  
    */
    public Integer getListed(){
    	return listed;
    }
    
    /**
    * Set the language in which the tweet has been written 
    * @param tmp language associated to the tweet 
    */
    public void setLanguage(String tmp){
    	language = tmp;
    }

    /**
    * @return the language associated to the tweet 
    */
    public String getLanguage(){
        return language;
    }
    
    /**
    * @return the url associated to the tweet 
    */
    public String getUrl(){
    	return url;
    }
    
    /**
    * Set the url of the twitter page of the user associated to the tweet 
    * @param tmp url to set 
    */
    public void setUrl(String tmp){
    	url = tmp;
    }
   
    /**
    * Set the number of the cluster in which the tweet is contained 
    * @param tmp number of the cluster 
    */
    public void setCluster(int tmp){
    	cluster = tmp;
    }
    
    /**
    * @return the number of the cluster associated to the tweet 
    */
    public int getCluster(){
    	return cluster;
    }
    
}//{c}Twitter 