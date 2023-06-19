# Last amended: 14th March, 2023
# My folder: /home/ashok/Documents/hadoop_streaming

# Good Refernce:
# https://www.inf.ed.ac.uk/teaching/courses/exc/labs/hadoop_streaming.html

# Objective:
# We read all data files that are in a folder
# and apply map-reduce on all the files.

# 0.0
cd /home/ashok/Documents/hadoop_streaming
chmod 777 *.py

# 1.1
cd ~
hdfs dfs -rm -r /user/ashok/data_files/streaming/student_out
hdfs dfs -mkdir -p hdfs://localhost:9000/user/ashok/data_files/streaming
hdfs dfs -put /home/ashok/Documents/hadoop_streaming/smarks.txt  /user/ashok/data_files/streaming




# 1.2 Location of hadoop jar file
HSTREAMING='/opt/hadoop-3.3.4/share/hadoop/tools/lib/hadoop-streaming-3.3.4.jar'

# 1.3
hadoop jar  $HSTREAMING \
    -input  /user/ashok/data_files/streaming \
    -output  /user/ashok/data_files/streaming/student_out  \
    -mapper /home/ashok/Documents/hadoop_streaming/mapper.py 

    

# 1.4
hdfs dfs -cat /user/ashok/data_files/streaming/student_out/part-00000    

# 1.5 OR
hadoop jar  $HSTREAMING \
    -input  /user/ashok/data_files/streaming/data.csv \
    -output  /user/ashok/data_files/streaming/student_out  \
    -mapper 'python /home/ashok/Documents/hadoop_streaming/python_mappers/mapper.py' \
    -reducer 'python /home/ashok/Documents/hadoop_streaming/python_mappers/reducer.py' 
   
hadoop jar  $HSTREAMING \
    -input  /user/ashok/data_files/streaming \
    -output  /user/ashok/data_files/streaming/student_out  \
    -mapper /home/ashok/Documents/hadoop_streaming/python_mappers/mapper.py \
    -reducer /home/ashok/Documents/hadoop_streaming/python_mappers/reducer.py 
######################################################## 
## Note the following outputs:
######################################################## 
## A.
Map-Reduce Framework
		Map input records=10                     <= 5 in data.csv and 5 in data1.csv
		Map output records=6                     <= 6 only where f2 > 20
		Map output bytes=72
		Map output materialized bytes=96
		Input split bytes=229
		Combine input records=0
		Combine output records=0
		Reduce input groups=1			<= Reducer was 1 
		Reduce shuffle bytes=96
		Reduce input records=6			<= Reducer has 6 records
		Reduce output records=1			<= Reducer output record is 1
		Spilled Records=12
		Shuffled Maps =2			
		Failed Shuffles=0
		Merged Map outputs=2

##B. How many mappers?

At localhost:8088, click on your 'application_...' hyperlink; then click on 'History'
hyperlink. There you will know that for this job there were two mappers and one reducer.

######################################################## 

