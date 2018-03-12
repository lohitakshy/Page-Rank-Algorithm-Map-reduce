How To run Programs :
Lohitaksh/lohit yogi 
800989207

PageRank.Java
compile the PageRank class

$ mkdir -p build

$ hadoop fs -put wiki-micro.txt /user/cloudera/PageRank/input

$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/ PageRank.java -d build -Xlint

create the JAR file for the application

$ jar -cvf PageRank.jar -C build/ .

Run the follwing 3 commands to make sure output files do not exist

$ hadoop fs -rm -r /user/cloudera/PageRank/output0 /user/cloudera/PageRank/output1 /user/cloudera/PageRank/output2

$ hadoop fs -rm -r /user/cloudera/PageRank/Nvalue

$ hadoop fs -rm -r /user/cloudera/PageRank/result

Run the PageRank application from the JAR file, passing the paths to the input and output directories in HDFS.

$ hadoop jar pagerank.jar org.myorg.PageRank /user/cloudera/PageRank/input /user/cloudera/PageRank/output0 


$ hadoop fs -cat /user/cloudera/PageRank/result/*





$ first MapReduce is used to calculate number of nodes

$ second MapReduce calculate do the extraction of links

$ Third Mapreduce calculating the first page rank 

$ Fourth Mapreduce sorting the output 