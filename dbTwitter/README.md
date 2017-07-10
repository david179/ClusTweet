### SQL
Check java version. 
If it is previous than java8 change postresql driver for java (jdbc) inside /libs/

The sql queries for creating the database and importing the cav files are inside /script/sql.
TODO: changing the path for the csv file which is inside the same folder.

### Compiling and executing with eclipse
1) Import the project as gradle project (it will handle all the external packages)
2) Modify the database information (database, username and password) inside the java class DbFunctions
3) Link the external library jdbs in /libs to the execution of the project
4) Executing

On default the program uses 8 core.
To modify this setting change the parametre .master on line 44 of file ClusTweets.java.

In order to execute again the code, please remove directory freq_nouns in the main directory and drop the entropy table in postgre database.

The output are three text files which will be written in the main directory.

### Compiling the code from command line
Execute the following commands in a shell
1) ./gradlew to download all the files needed
2) ./gradlew ShadowJar
3) ./script/java/executing.sh