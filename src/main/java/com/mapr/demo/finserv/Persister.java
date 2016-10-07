/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.demo.finserv;

/******************************************************************************
 * PURPOSE:
 *   This Kafka consumer reads NYSE Tick data from a MapR Stream topic and
 *   persists each message in a MapR-DB table as a JSON Document, which can
 *   later be queried using Apache Drill (for example).
 *
 * EXAMPLE USAGE:
 *   java -cp ~/nyse-taq-streaming-1.0.jar:$CP com.mapr.demo.finserv.Persister /user/mapr/taq:sender_1361
 *
 * EXAMPLE QUERIES FOR MapR dbshell:
 *      mapr dbshell
 *          find /user/mapr/ticktable
 *
 * EXAMPLE QUERIES FOR APACHE DRILL:
 *      /opt/mapr/drill/drill-1.6.0/bin/sqlline -u jdbc:drill:
 *          SELECT * FROM dfs.`/mapr/ian.cluster.com/user/mapr/ticktable`;
 *          SELECT * FROM dfs.`/user/mapr/ticktable`;
 *
 *****************************************************************************/

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.QueryCondition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.HashSet;
import java.util.Set;

public class Persister {

	// Declare a new consumer.
	private static KafkaConsumer consumer;

	// every N rows print a MapR-DB update
	private static int U_INTERVAL = 100;

	// polling
	private static int TIMEOUT = 1000;


	public static void main(String[] args) throws IOException {
		configureConsumer(args);

		// we will listen to everything in JSON format after it's
		// been processed
		String topic = "/user/mapr/taq:processed";
		String tableName = "/user/mapr/ticktable";
		long nmsgs = 0;
		Table table;

		if (args.length == 1) {
			topic = args[0];
		}

		List<String> topics = new ArrayList<String>();
		topics.add(topic);

		// subscribe to the raw data
		System.out.println("Subscribing to " + topic);
		consumer.subscribe(topics);

		// delete the old table if it's there
		if (MapRDB.tableExists(tableName)) {
			System.out.println("deleting old table " + tableName);
			MapRDB.deleteTable(tableName);
		}

		// make a new table
		table = MapRDB.createTable(tableName);

		// probably want this
		table.setOption(Table.TableOption.BUFFERWRITE, true);

		// request everything
		for (;;) {
			ConsumerRecords<String, byte[]> msg = consumer.poll(TIMEOUT);
			if (msg.count() == 0) {
				System.out.println("No messages after 1 second wait.");
			} else {
				System.out.println("Read " + msg.count() + " messages");
				nmsgs += msg.count();

				// Iterate through returned records, extract the value
				// of each message, and print the value to standard output.
				Iterator<ConsumerRecord<String, byte[]>> iter = msg.iterator();
				while (iter.hasNext()) {
					ConsumerRecord<String, byte[]> record = iter.next();

					Tick tick = new Tick(record.value());
					Document document = MapRDB.newDocument((Object)tick);

					// save document into the table
					table.insertOrReplace(tick.getTradeSequenceNumber(), document);
				}
			}
		}
	}

	/* Set the value for configuration parameters.*/
	private static void configureConsumer(String[] args) {
		Properties props = new Properties();
		// cause consumers to start at beginning of topic on first read
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		//  which class to use to deserialize the value of each message
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.ByteArrayDeserializer");

		consumer = new KafkaConsumer<String, String>(props);
	}
}
