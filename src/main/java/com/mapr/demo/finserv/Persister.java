/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.demo.finserv;

/******************************************************************************
 * PURPOSE:
 *   This Kafka consumer tails NYSE Tick data from a MapR Stream topic and
 *   persists each message in a MapR-DB table as a JSON Document which can
 *   later be queried using Apache Drill (for example).
 *
 * EXAMPLE USAGE:
 *   java -cp ~/nyse-taq-streaming-1.0.jar:$CP com.mapr.demo.finserv.Persister -topics /user/mapr/taq:sender_1361,/user/mapr/taq:sender_0410 -table /user/mapr/ticktable -droptable -verbose
 *
 * EXAMPLE QUERIES FOR MapR dbshell:
 *      mapr dbshell
 *          find /user/mapr/ticktable
 *
 * EXAMPLE QUERIES FOR APACHE DRILL:
 *      /opt/mapr/drill/drill-1.6.0/bin/sqlline -u jdbc:drill:
 *          SELECT count(*) FROM dfs.`/user/mapr/ticktable`;
 *          SELECT * FROM dfs.`/mapr/ian.cluster.com/user/mapr/ticktable` limit 10;
 *          SELECT sender, count(1) TradesExecuted FROM dfs.`/user/mapr/ticktable` group by sender order by TradesExecuted desc limit 10
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
import java.util.*;

public class Persister {

	public static String tableName = null;
	public static Boolean VERBOSE = false;
	public static Boolean DROPTABLE = false;

	// Declare a new consumer.
	private static KafkaConsumer consumer;

	// polling
	private static int TIMEOUT = 10000;

	public static void usage() {
		System.err.println("Persister ");
		System.err.println("     -topics <comma separated stream:topic names>");
		System.err.println("     -table <target MapR-DB table name>");
		System.err.println("     [-droptable]");
		System.err.println("     [-verbose]");
		System.exit(1);
	}

	public static void main(String[] args) throws IOException {
		List<String> topics = new ArrayList<String>();

		for (int i = 0; i < args.length; ++i) {
			if (args[i].equals("-table")) {
				i++;
				if (i >= args.length) usage();
				tableName = args[i];
			} else if (args[i].equals("-topics")) {
				i++;
				if (i >= args.length) usage();
				topics = Arrays.asList(args[i].split(","));
			} else if (args[i].equals("-droptable")) {
				i++;
				DROPTABLE = true;
			} else if (args[i].equals("-verbose")) {
				i++;
				VERBOSE = true;
			}
		}

		if (tableName == null) {
			System.err.println("Table name is mandatory command-line option");
			usage();
		}
		if (topics.size() == 0) {
			System.err.println("Topic is mandatory command-line option");
			usage();
		}

		configureConsumer(args);

		long nmsgs = 0;
		Table table = null;

		// subscribe to the raw data
		System.out.println("subscribing to " + topics);
		consumer.subscribe(topics);

		// delete the old table if it already exists
		if (MapRDB.tableExists(tableName) && DROPTABLE == true) {
			System.out.println("deleting old table " + tableName);
			MapRDB.deleteTable(tableName);
		}

        if (!MapRDB.tableExists(tableName)) {
            System.out.println("creating table " + tableName);
            table = MapRDB.createTable(tableName);
            table.setOption(Table.TableOption.BUFFERWRITE, true);
        }

        if (table == null) {
		    System.err.println("Error creating table " + tableName);
            System.exit(1);
        }

		// request everything
		System.out.println("working...");
		if (VERBOSE)
			System.out.println("Waiting for new messages...");
		Boolean show_status = false;
		for (;;) {
			ConsumerRecords<String, byte[]> msg = consumer.poll(TIMEOUT);
			if (msg.count() == 0) {
				if (show_status) {
					System.out.println("Waiting for new messages...");
					show_status = false;
				}
			} else {
				if (VERBOSE) {
					show_status = true;
					System.out.println("Read " + msg.count() + " messages");
					nmsgs += msg.count();
				}

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
		props.put("auto.offset.reset", "latest");
		props.put("group.id", "maprdb-feeder");
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		//  which class to use to deserialize the value of each message
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.ByteArrayDeserializer");

		consumer = new KafkaConsumer<String, String>(props);
	}
}
