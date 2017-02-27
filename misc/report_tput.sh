#!/bin/bash
DATAPOINT_PATH='/opt/zeppelin-0.7.0-SNAPSHOT/webapps/webapp/data.html'
DATAPOINTS='/home/mapr/tput_data.dat'

while true; do
    ps auxw |grep -v grep | grep record_tput.sh 
    if [ $? -ne 0 ]; then break; fi
    sleep 1
    new=`tail -n 1 $DATAPOINTS`
    TS=`date +%s`
    DP=`expr $TS \* 1000 \- 28800 \* 1000`
    echo "[$DP, $new]" > $DATAPOINT_PATH
done
# replay for 60 seconds
end=$((SECONDS+60))
while [ $SECONDS -lt $end ]; do
    cat $DATAPOINTS | while read line; do
        TS=`date +%s`
        DP=`expr $TS \* 1000 \- 28800 \* 1000`
        echo "[$DP, $line]" > $DATAPOINT_PATH
        sleep 1
	if [ $SECONDS -gt $end ]; then break; fi
    done
done
TS=`date +%s`
DP=`expr $TS \* 1000 \- 28800 \* 1000`
echo "[$DP, null]" > $DATAPOINT_PATH
