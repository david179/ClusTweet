-- create the extension for the foreign data wrapper file_fdw 
CREATE EXTENSION file_fdw; 
-- create the server for the foreign data wrapper 
CREATE SERVER server_file_fdw 
FOREIGN DATA WRAPPER file_fdw; 

CREATE USER MAPPING FOR postgres
SERVER server_file_fdw; 


--nuovo 
DROP FOREIGN TABLE IF EXISTS tweet_localUSA ; 

CREATE FOREIGN TABLE tweet_localusa(

    tweet_ID varchar(150) ,
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
    url varchar(200) ) SERVER server_file_fdw  

OPTIONS (format 'csv', header 'true' , filename '/home/tom/Documents/dbAlessandro/src/main/sql/lista_tweets_USA.csv' , delimiter ',' );


CREATE TABLE tweets_localusa
AS SELECT * FROM tweet_localusa ; 


