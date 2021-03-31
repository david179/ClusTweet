
-- Return the number of tweets released by each user 
SELECT T.username, COUNT(T.username)
FROM tweets_localusa3 AS T 
GROUP BY T.username; 
--Total number of users in the dataset : 76,234 


SELECT RES.US 
FROM (
       SELECT T.username AS US, COUNT(T.username) AS NUM
       FROM tweets_localusa3 AS T 
       GROUP BY T.username 
     ) AS RES 

WHERE RES.NUM = 1 ; 
--Number of users which have released only a tweet : 51,686 


SELECT RES.US 
FROM (
       SELECT T.username AS US, COUNT(T.username) AS NUM
       FROM tweets_localusa3 AS T 
       GROUP BY T.username 
     ) AS RES 

WHERE RES.NUM > 5 ; 

-- NUMBER OF USERS = 4300 


SELECT RES.US 
FROM (
       SELECT T.username AS US, COUNT(T.username) AS NUM
       FROM tweets_localusa3 AS T 
       GROUP BY T.username 
     ) AS RES 

WHERE RES.NUM > 10; 

-- NUMBER OF USERS 2044 


-- Scoprire quanti tweets ha rilasciato un utente in relazione al numero di followers 
SELECT TW.username, TW.followers 
FROM ( 
       SELECT RES.US AS R 
       FROM (
              SELECT T.username AS US, COUNT(T.username) AS NUM
              FROM tweets_localusa3 AS T 
              GROUP BY T.username 
            ) AS RES 
       WHERE RES.NUM = 1  

     ) AS S INNER JOIN tweets_localusa3 AS TW ON S.R=TW.username 
WHERE TW.followers > 2897 ; 

/*
   4260 users have a number of followers higher than the average but they will appear 
   inside only one cluster because each of them has only one tweet inside the database.  
*/

--total number of users with a number of followers higher than the average 
SELECT DISTINCT T.username 
FROM tweets_localusa3 AS T 
WHERE T.followers > 2897 ; 

-- Total number of users with a number of followers greater than 2897 = 6526 
-- And 4260 users out of 6526 have released only one tweet!! ---> 
-- It's impossible to do a clustering based on the number of followers per user!! 


SELECT AVG(S.F) AS AV 
FROM ( 
       SELECT T.username, AVG(T.followers) AS F 
       FROM tweets_localusa3 AS T 
       GROUP BY T.username
     ) AS S ;

-- AVG(T.followers)= followers for each user--> inserted into an AVG clause because we cannot report a value in the 
-- SELECT clause which is not contained in the GROUP BY clause 

-- Average = 2897 

 
