
-- NEW TWITTER fdw 

-- create the extension for the foreign data wrapper file_fdw 
CREATE EXTENSION file_fdw; 

-- create the server for the foreign data wrapper 
CREATE SERVER server_file_fdw 
FOREIGN DATA WRAPPER file_fdw; 


--CREATE THE FOREIGN TABLE 
DROP FOREIGN TABLE IF EXISTS tweet_usa; 

CREATE FOREIGN TABLE tweet_usa(

    tweet_ID varchar(150) ,
    datetweet varchar(50),  
    hour varchar(50),  
    username varchar(50) ,  
    nickname varchar(50),  
    biography varchar(250),  
    tweet_content varchar(300) ,  
    favs varchar(150),  
    rts varchar(50),  
    latitude varchar(200) ,
    longitude varchar(200) , 
    country varchar(200) ,  
    place varchar(200),  
    profile_picture varchar(200),  
    followers integer, 
    following integer,  
    listed integer,   
    language varchar(10), 
    url varchar(200) ) SERVER server_file_fdw  

OPTIONS (format 'csv', header 'true' , filename '/tmp/lista_tweets_USA.csv' , delimiter ',' , null '' );


--Creation of the local table, in which we save all the data contained inside the foreign table 
CREATE TABLE tweets_localusa
AS SELECT * FROM tweet_usa ; 

--Remove the rows of the local table with followers, following or listed equal to NULL 
DELETE 
FROM tweets_localusa AS T 
WHERE T.followers IS NULL OR T.following IS NULL OR T.listed IS NULL; 


--Create the output table with the cluster indexes 
CREATE TABLE clusters
(
  tweet_ID varchar(150) ,
  cluster int,
  datetweet varchar(50),  
  hour varchar(50),  
  username varchar(50) ,  
  nickname varchar(50),  
  biography varchar(250),  
  tweet_content text,  
  favs varchar(150),  
  rts varchar(50),  
  latitude varchar(200) ,
  longitude varchar(200) , 
  country varchar(200) ,  
  place varchar(200),  
  profile_picture varchar(200),  
  followers integer, 
  following integer,  
  listed integer,   
  language varchar(10), 
  url varchar(200),
  primary key (tweet_ID) 
);



