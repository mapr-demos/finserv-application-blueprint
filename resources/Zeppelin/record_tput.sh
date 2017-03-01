#!/bin/bash
DATAPOINTS='/home/mapr/tput_data.dat'

echo 0 > $DATAPOINTS
while read -r line; do
    if grep -q Throughput <<< $line; then
        new=$(echo "$line" | sed 's/Throughput = \([0-9.]*\) Kmsgs.*/\1/')
        echo "$new" >> $DATAPOINTS
    fi
done

