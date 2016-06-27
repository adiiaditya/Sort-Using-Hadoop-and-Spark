# Sort-Using-Hadoop-and-Spark

This project was created as a part of coursework "Cloud Computing" at Illinois Institute of Technology at Chicago

Compilation:

javac -d hadoopsort HadoopSort.java
jar -cvf hadoopsort.jar -c hadoopsort/ .

Run:

hadoop-2.7.2/bin/hadoop jar hadoopsort.jar org.myorg.HadoopSort /data/input /data/output
