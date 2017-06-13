The code is ready to be built with Gradle.

The main class is inside src/, main.java.it.unipd.dei.dm1617.KCenterMapReduceV5
It needs as input parametres:
1. the path of the dataset 
2. the number of clusters

JVM also need where to run the code so this parameter is need <-Dspark.master="local"> (maximum number of core).
On the other hand it return in the standard output a list of the titles of the documents grouped by cluster.
Finally there is a print of the objective function.

Pay attention on running the program twice without deleting the folder "Model/":
it is created in the first run and it contains the model for the training;
the main problem is that it cannot be overwrite and Java throw an exception.
