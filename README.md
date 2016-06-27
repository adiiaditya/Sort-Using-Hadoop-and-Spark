# Sort-Using-Hadoop-and-Spark

Compilation:

javac -d hadoopsort HadoopSort.java
jar -cvf hadoopsort.jar -c hadoopsort/ .

Run:

hadoop-2.7.2/bin/hadoop jar hadoopsort.jar org.myorg.HadoopSort /data/input /data/output
