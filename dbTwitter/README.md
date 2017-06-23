### SQL
Check java version. 
If it is previous than java8 change postresql driver for java (jdbc) inside /build/libs/

The sql instruction for creating the database and importing the cav files are inside /script/sql.
TODO: changing the path for the csv file which is inside /src/main/sql.

### Compiling the code
1) ./gradlew to download all the files needed
2) ./gradlew ShadowJar
3) ./script/java/executing.sh