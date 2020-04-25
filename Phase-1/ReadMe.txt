TeamMates

Procedure:

1. Hadoop Installation
	Install Homebrew
	Install & configure java(JDK)
	configure SSH
	Install Apache Hadoop using brew install wget, version 3.2.1 binary format(.gz file).
	Extract the .gz file
	configure ./bashrc, core-site.xml, hdfs-site.xml, yarn-site.xml, mapred-site.xml, hadoop-env.sh
	format namenode
	start the services of hadoop
	check whether it got installed using hadoop version command
	
2. Apache Spark Installation
	Install xcode-select
	Install Scala
	Install Spark using (brew install apache-spark)
	verify installation using spark-shell command.
	
3. Tweepy Installation
	Install from source:
		git clone git://github.com/tweepy/tweepy.git
		cd tweepy
		python setup.py install
		
3. Twitter Developer account access
	Sign Up for Twitter.com
	Raise a request for Developer Access for developer.twitter.com with Student Email Address(@mail.umkc.edu)
	Create a New Application
		Goto My Applciations
		Create a New Applciation
		Fill the required Details
		create/generate the token
		
4. Python program to fetch the tweets using twitter API keys
	We have written a python program for fetching of twitter api data.
	It downloaded 1.68GB of twitter data
	Number of Tweets Collected in the file: 1,75,537
	Source Code type: .py file
	output file type: .txt file
	
5. Python program to extract Hashtags and URLs
	we have written python code to fetch Hashtags & URLS from the downloaded twitter data(.txt), with the previous program output file as input to this program.
	Source code type: .py file
	output file type: .txt file
	
6. Inserting Data into HDFS
	Create a directory to place the files using mkdir
	copy files from local machine into HDFS.(using put/copyFromLocal)
	check whether it has moved using ls command
		
7. Executing Word Count Program in Apache Hadoop
	We have created a .jar file using java program for the execution of the Word Count.
	Input file type: .txt(generated in the step 5)
	Output file type: .txt
	Output content is the key, count of the key
	
8. Executing Word Count Program in Apache Spark
	Scala is used for the word count program in Apache Spark
	var map = sc.textFile("output file from step 5").flatMap(line => line.split(" ")).map(word => (word,1));
	var counts = map.reduceByKey(_ + _);
	counts.saveAsTextFile("Output file path")
	Input file type: .txt(generated in the step 5)
	Output Files generated: part-00000, part-00001_SUCCESS
