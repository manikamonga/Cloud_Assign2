# Cloud_Assign2

Procedure to Configure
Step 1: AWS account was created.
Step 2: To setup the clusters on AWS, install hadoop and spark on the namenode and datanode, a script was run. The link of its github repository is given in the reference section. [Please see point 3].

Procedure to Compile and make a jar file
Step 1: All codes were written in Scala, in Eclipse. 
Step 2: Scala plugin was installed in eclipse and some scala compatible jar files had to be exported for successful build.
Step 3: After the successful testing, the jar file of the code was built and added to the Hadoop server using the following command:
scp -i ./SparkKey1.pem /Users/manikamonga/Documents/CloudAssign2/CloudAssign2.jar root@ec2-34-206-64-207.compute-1.amazonaws.com:~

Procedure to make and view the input file
Step 1: Make an input directory in the hadoop cluster using the following commands
./hadoop fs -copyFromLocal /volume-data/rawd/freebase-wex-2009-01-12-articles.tsv /input-data
Step 2: The output directory is automatically created. Therefore there is no need to pre-define it.

Procedure to execute jar file on AWS cluster
Step 1: Open Terminal and log onto your namenode using “ssh namenode”.
Step 2: To confirm successful transmission of the jar file onto the server use “ls” command. The jar file will be listed.
Step 3 : cd spark. This gets you in the spark directory.
Step 4: To run the jar file, do the following separately for each Scala object.
./bin/spark-submit --class PageRank --master <master-url> ~/CloudAssign2.jar 2
This executes the PageRank object with 15 iterations. Use the command above for other object execution.

Procedure to view the contents of the output file
Step 1: execute the following commands
Cd ephemeral-hdfs
./hadoop fs -cat /output-data/part-00001
