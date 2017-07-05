package it.unipd.dei.db;


import java.io.Serializable;

public class Twitter implements Serializable
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

    public Twitter(){
    	super();
    }
    
    // costruttore della classe 
    public Twitter(String tweet_ID,String dateT,String hour,String username,String nickname, String biography, String tweet_content,String favs, String rts, String latitude, String longitude, String country, String place, String prof, int followers, int following, int listed,  String language, String url)
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
    }
    
	public void printTwitterInfo() 
	{
	   System.out.println("ID: " + tweet_ID + " , Date : " + dateTweet + " , URL : " + url + " , country : " + country ); 
	}
    
    public void setTweet_ID(String tmp){
    	tweet_ID = tmp;
    }
    
    public String getTweet_ID(){
    	return tweet_ID;
    }
    
    public void setDateTweet(String tmp){
    	dateTweet = tmp;
    }
    
    public String getDateTweet(){
    	return dateTweet;
    }
    
    public void setHour(String tmp){
    	hour = tmp;
    }
    
    public String getHour(){
    	return hour;
    }
    
    public void setUsername(String tmp){
    	username = tmp;
    }
    
    public String getUsername(){
    	return username;
    }
    
    public void setNickname(String tmp){
    	nickname = tmp;
    }
    
    public String getNickname(){
    	return nickname;
    }
    
    public void setBiography(String tmp){
    	biography = tmp;
    }
    
    public String getBiography(){
    	return biography;
    }
    
    public void setTweet_content(String tmp){
    	tweet_content = tmp;
    }
    
    public String getTweet_content(){
    	return tweet_content;
    }
    
    public void setFavs(String tmp){
    	favs = tmp;
    }
    
    public String getFavs(){
    	return favs;
    }
    
    public void setRts(String tmp){
    	rts = tmp;
    }
    
    public String getRts(){
    	return rts;
    }
    
    public void setLatitude(String tmp){
    	latitude = tmp;
    }
    
    public String getLatitude(){
    	return latitude;
    }
    
    public void setLongitude(String tmp){
    	longitude = tmp;
    }
    
    public String getLongitude(){
    	return longitude;
    }
    
    public void setCountry(String tmp){
    	country = tmp;
    }
    
    public String getCountry(){
    	return country;
    }
    
    public void setPlace(String tmp){
    	place = tmp;
    }
    
    public String getPlace(){
    	return place;
    }
    
    public void setProfile_picture(String tmp){
    	profile_picture = tmp;
    }
    
    public String getProfile_picture(){
    	return profile_picture;
    }
    
    public void setFollowers(Integer tmp){
    	followers = tmp;
    }
    
    public Integer getFollowers(){
    	return followers;
    }
    
    public void setFollowing(Integer tmp){
    	following = tmp;
    }
    
    public Integer getFollowing(){
    	return following;
    }
    
    public void setListed(Integer tmp){
    	listed = tmp;
    }
    
    public Integer getListed(){
    	return listed;
    }
    
    public String getLanguage(){
    	return language;
    }
    
    public void setLanguage(String tmp){
    	language = tmp;
    }
    
    public String getUrl(){
    	return url;
    }
    
    public void setUrl(String tmp){
    	url = tmp;
    }

}//{c}Twitter 
