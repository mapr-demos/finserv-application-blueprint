/*
 * Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved
 * 
 * This class is provided as a teaching example to persist data into MapR-DB, and is not
 * run as part of the main demo.
 */
package com.mapr.demo.finserv;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.ojai.exceptions.DecodingException;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.QueryCondition;
import java.util.*;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Persister {

	private static final Logger LOG = LoggerFactory.getLogger(Persister.class);

	// Declare a new consumer.
	private static KafkaConsumer consumer;

	// every N rows print a MapR-DB update
	private static final int U_INTERVAL = 100;

	// polling
	private static final int TIMEOUT = 1000;

	public static void main(String[] args) throws IOException {
		configureConsumer(args);

		// we will listen to everything in JSON format after it's
		// been processed
		String topic = "/user/mapr/taq:processed";
		String tableName = "/user/mapr/ticktable";
		Set<String> syms = new HashSet<>();
		long nmsgs = 0;
		Table table;

		if (args.length == 1) {
			topic = args[0];
		}

		List<String> topics = new ArrayList<>();
		topics.add(topic);

		// subscribe to the raw data
		System.out.println("subscribing to " + topic);
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
			// Request unread messages from the topic.
			ConsumerRecords<String, byte[]> records;
			records = consumer.poll(TIMEOUT);
			if (records == null || records.count() == 0) {
				System.out.println("The " + topic + "topic is currently empty, no records");
				continue;
			}
			try {
				for (ConsumerRecord<String, byte[]> raw_record : records) {
					String key = Long.toString(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTimeInMillis());
                    byte[] data = raw_record.value();


                    Tick writeTick = new Tick(data);
                    syms.add(writeTick.getSymbolRoot());

                    // This is just one way to do CRUD operations, for more examples, see:
                    // http://maprdocs.mapr.com/home/MapR-DB/JSON_DB/creating_documents_with_maprdb_ojai_java_api_.html
                    // https://github.com/mapr-demos/maprdb-ojai-101

                    try {
                        Document document = MapRDB.newDocument(new ObjectMapper().writeValueAsString(writeTick));
                        table.insertOrReplace(writeTick.getTradeSequenceNumber(), document);
                    } catch (DecodingException e) {
                            System.err.println("decoding exception");
                    }
                    nmsgs++;
			    }
            } catch (StringIndexOutOfBoundsException e) {
				System.err.println("Invalid record");
			}
			if ((nmsgs % U_INTERVAL) == 0) {
				System.out.println("Write update per-symbol:");
				System.out.println("------------------------");

				for (String s : syms) {
					QueryCondition cond = MapRDB.newCondition()
						.is("symbolRoot", QueryCondition.Op.EQUAL, s).build();
					DocumentStream results = table.find(cond);
					int c = 0;
					for (Document d : results) {
						c++;
					}
					System.out.println("\t" + s + ": " + c);
				}
			}
		}
    }

	/*
	 * cause consumers to start at beginning of topic on first read
	 */
	private static void configureConsumer(String[] args) {
		Properties props = new Properties();
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		consumer = new KafkaConsumer<>(props);
	}
}
