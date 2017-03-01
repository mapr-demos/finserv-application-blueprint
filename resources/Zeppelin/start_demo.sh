#!/bin/bash

PRODUCERHOST='nodea'
CONSUMERHOST='nodeb'

# kill off any producers or consumers running
ssh mapr@$PRODUCERHOST  "pkill -TERM -f 'java.*com.mapr.demo.finserv.Run'"
ssh mapr@$CONSUMERHOST  "pkill -TERM -f 'java.*com.mapr.demo.finserv.Run'"

# run the spark job to copy ticks to Hive table -- do this with at(1) to avoid
# waiting for the child process to finish
ssh mapr@$CONSUMERHOST 'rm -f /tmp/spark.log; touch /tmp/spark.log' > /dev/null 2>&1
#ssh mapr@$CONSUMERHOST "hive -e 'truncate table streaming_ticks2;'" 2> /dev/null
#echo "ssh mapr@$CONSUMERHOST '/opt/mapr/spark/spark-*/bin/spark-submit --class com.mapr.demo.finserv.SparkStreamingToHive /home/mapr/nyse-taq-streaming-1.0.jar /user/mapr/taq:trades streaming_ticks2 &> /tmp/spark.log'" | at -M now > /dev/null 2>&1
# We should wait for the spark consumer is waiting for messages before we start the producer (but this can slow down the script startup by 45 seconds)

###ssh mapr@$CONSUMERHOST 'grep "Waiting for messages..." /tmp/spark.log' > /dev/null; while [ $? -eq 1 ]; do sleep 1; echo -n "."; ssh mapr@$CONSUMERHOST 'grep "Waiting for messages..." /tmp/spark.log'; done &


# run the producer on the first host -- do this with at(1) to avoid
# waiting for the child process to finish
#ssh mapr@$PRODUCERHOST 'ls -l  /home/mapr/finserv-data-files/1minute | grep v1; if [ $? -eq 1 ]; then ln -fs  /home/mapr/finserv-data-files/1minute_v1  /home/mapr/finserv-data-files/1minute; else ln -fs  /home/mapr/finserv-data-files/1minute_v2  /home/mapr/finserv-data-files/1minute; fi'
echo "ssh mapr@$PRODUCERHOST  'java -cp `mapr classpath`:/home/mapr/finserv-application-blueprint/target/nyse-taq-streaming-1.0.jar \
     com.mapr.demo.finserv.Run producer \
     /home/mapr/finserv-data-files/data/ /user/mapr/taq:trades 2>&1' | \
     /home/mapr/record_tput.sh" | at -M now
# run the consumer on the other host
ssh -n -f mapr@$CONSUMERHOST  "sh -c 'nohup java -cp `mapr classpath`:/home/mapr/finserv-application-blueprint/target/nyse-taq-streaming-1.0.jar:/home/mapr/finserv-application-blueprint/src/test/resources \
    com.mapr.demo.finserv.Run consumer \
    /user/mapr/taq:trades 3 < /dev/null > std.out 2> std.err &'"

CONSPID=$(ssh mapr@$CONSUMERHOST  ps auxw | grep 'consumer' | \
    grep -v grep | awk ' { print $2 } ')
PRODPID=$(ssh mapr@$PRODUCERHOST  ps auxw | grep 'Run producer' | \
    grep -v grep | head -1 | awk ' { print $2 } ')

/home/mapr/report_tput.sh > /dev/null 2>&1 &

echo "%html"
echo "Producer started: <font color='green'> PID $PRODPID </font><br>"
echo "Consumer started: <font color='green'> PID $CONSPID </font>"

# sleep 1
# # run the table gen locally
# /opt/mapr/spark/spark-1.6.1/bin/spark-submit
# --class com.mapr.demo.finserv.SparkStreamingConsole
# /mapr/my.cluster.com/user/mapr/nyse-taq-streaming-1.0-jar-with-dependencies.jar /user/mapr/taq:sender_1361 < /dev/null > std.out 2> std.err &

