## Last amended: 31st January, 2018
## My folder: /home/ashok/Documents/spark/libsvm_hadoop_streaming
## Ref:  file:///home/ashok/hadoop/share/doc/hadoop/hadoop-mapreduce-client/hadoop-mapreduce-client-core/HadoopStreaming.html
##
##
## Objective: 
##          Learn how to use hadoop-streaming
##          Transform a hadoop csv file to libsvm format
##
##
#  Upload requisite data file after deleting existing folder
#    Data file: smarks.txt ; Target variable: Col 2
#
#	cd ~
#	hdfs dfs -rm -r /user/ashok/data_files/streaming/student_out
#	hdfs dfs -rm -r /user/ashok/data_files/streaming
#	hdfs dfs -mkdir /user/ashok/data_files/streaming
#	hdfs dfs -put /home/ashok/Documents/spark/libsvm_hadoop_streaming/smarks.txt /user/ashok/data_files/streaming
#	hdfs dfs -cat /user/ashok/data_files/streaming/smarks.txt
#


## Steps:
#		1. Mapper simply outputs line-by-line using 'cat'
#		2. Reducer applies 'awk' script to write libsvm format, line by line
#		3. Hadoop jar files sees to it that both mapper and reducers are fed (key:value) pairs


## libsvm Program script

# 1.0 Where is my jar file on my machine? Depends upon your hadoop version.
# HSTREAMING='/home/ashok/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar'
HSTREAMING='/opt/hadoop-3.1.1/share/hadoop/tools/lib/hadoop-streaming-3.1.1.jar'


# 2.0 Execute the jar file with needed arguments
hadoop jar $HSTREAMING \
    -D mapreduce.job.name='ConvertTo_libsvm' \
    -input /user/ashok/data_files/streaming/smarks.txt \
    -output /user/ashok/data_files/streaming/student_out \
    -mapper /bin/cat \
    -reducer /home/ashok/Documents/spark/libsvm_hadoop_streaming/smarks.awk

    
# 3.0 Check the output
hdfs dfs -cat /user/ashok/data_files/streaming/student_out/part-00000

##########    

# Example 2
# Count lines, bytes and words in a document
# Test as follows:
#
#       cd /home/ashok/Documents/hadoop_streaming
#	cat howStreamingWorks.txt |  wc

#   Initial actions
#   1. Delete the output folder, if it exists:
#        hdfs dfs -rm -r /user/ashok/data_files/hsw/

#   2. Copy the .txt file to hadoop
#   hdfs dfs -put /home/ashok/Documents/hadoop_streaming/howStreamingWorks.txt  /user/ashok/data_files/

#   3. Check if file has been copied:
#   hdfs dfs -ls  /user/ashok/data_files/


# 1.0 Where is the requisite jar file?
HSTREAMING='/opt/hadoop-3.1.1/share/hadoop/tools/lib/hadoop-streaming-3.1.1.jar'

# 1.1 map-reduce actions now
hadoop jar $HSTREAMING \
    -input /user/ashok/data_files/howStreamingWorks.txt \
    -output /user/ashok/data_files/hsw \
    -mapper /bin/cat \
    -reducer /usr/bin/wc

# 1.2 Check results as:

 $  hdfs dfs -cat /user/ashok/data_files/hsw/part-00000

########################################################################
    
