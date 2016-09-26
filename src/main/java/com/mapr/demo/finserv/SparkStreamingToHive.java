package com.mapr.demo.finserv;

/**
 * ****************************************************************************
 * PURPOSE: This spark application consumes records from Kafka topics and continuously persists each message in a Hive
 * table, for the purposes of analysis and visualization in Zeppelin.
 * <p>
 * EXAMPLE USAGE: /opt/mapr/spark/spark-1.6.1/bin/spark-submit --class com.mapr.demo.finserv.SparkStreamingToHive
 * /mapr/tmclust1/user/mapr/nyse-taq-streaming-1.0-jar-with-dependencies.jar /user/mapr/taq:sender_1142
 * <p>
 * EXAMPLE QUERIES FOR APACHE ZEPPELIN: Here are a few sample queries for interactively analyzing NYSE TAQ data. Log
 * into Zeppelin (default port 7000) and paste these commands as paragraphs in the notebook. %sql SELECT sender, symbol,
 * count(1) num_trades FROM streaming_ticks where symbol ="AA" group by sender, symbol order by sender %sql SELECT
 * price, volume, count(1) value FROM streaming_ticks where sender = "1361" group by price, volume, sender order by
 * price %sql select count(*) from streaming_ticks
 * <p>
 * REFERENCES: Spark Streaming API - http://spark.apache.org/docs/1.6.0/streaming-kafka-integration.html Spark Streaming
 * Guide - http://spark.apache.org/docs/1.6.0/streaming-programming-guide.html
 * <p>
 * AUTHOR: Ian Downard, idownard@mapr.com
 * <p>
 ****************************************************************************
 */
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.v09.KafkaUtils;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkStreamingToHive {

	private static final Logger LOG = LoggerFactory.getLogger(SparkStreamingToHive.class);

	private static final int NUM_THREADS = 1;

	// Seconds at which streaming data will be divided into batches
	private static final int BATCH_INTERVAL = 5000;

	// Hive table name for persisted ticks
	private static String HIVE_TABLE = "streaming_ticks";

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("ERROR: You must specify the stream:topic.");
			System.err.println("USAGE:\n"
				+ "\t/opt/mapr/spark/spark-1.6.1/bin/spark-submit --class com.mapr.demo.finserv.SparkStreamingToHive target/nyse-taq-streaming-1.0-jar-with-dependencies.jar stream:topic [hive table name]\n"
				+ "EXAMPLE:\n"
				+ "\t/opt/mapr/spark/spark-1.6.1/bin/spark-submit --class com.mapr.demo.finserv.SparkStreamingToHive /mapr/ian.cluster.com/user/mapr/nyse-taq-streaming-1.0-jar-with-dependencies.jar /user/mapr/taq:sender_1142 streaming_ticks");
			System.exit(-1);
		}

		SparkConf conf = new SparkConf()
			.setAppName("TAQ Spark Streaming")
			.setMaster("local[" + NUM_THREADS + "]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(BATCH_INTERVAL));
		HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(sc.sc());

		String topic = args[0];
		Set<String> topics = Collections.singleton(topic);

		if (args.length == 2)
			HIVE_TABLE = args[1];

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

		// Generate the schema for the dataframe that we will be constructing.
		// NOTE, if you change this schema remember to remove the Hive table
		// saved on disk or Spark will fail to update that table in foreachRDD.
		List<StructField> fields = new ArrayList<StructField>();
		fields.add(0, DataTypes.createStructField("date", DataTypes.TimestampType, true));
		fields.add(1, DataTypes.createStructField("symbol", DataTypes.StringType, true));
		fields.add(2, DataTypes.createStructField("price", DataTypes.DoubleType, true));
		fields.add(3, DataTypes.createStructField("volume", DataTypes.DoubleType, true));
		fields.add(4, DataTypes.createStructField("sender", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("receivers",
			DataTypes.createArrayType(DataTypes.StringType, true), true));

		StructType schema = DataTypes.createStructType(fields);

		// Create an input stream that directly pulls messages from Kafka Brokers without using any receiver.
		// This will query Kafka for the latest offset in each topic+parition every BATCH_INTERVAL seconds.
		JavaPairInputDStream<String, byte[]> directKafkaStream = KafkaUtils.createDirectStream(ssc,
			String.class, byte[].class, kafkaParams, topics);

		System.out.println("Waiting for messages...");

		// This foreachRDD is an output operator with at-least once semantics.
		// In case of worker failure we may persist duplicate data to Hive.
		// Reference: http://spark.apache.org/docs/latest/streaming-programming-guide.html#semantics-of-output-operations
		directKafkaStream.foreachRDD(rdd -> {
			if (rdd.count() > 0) {
				System.out.println("RDD batch size = " + rdd.count());
				// Parse each row using the helper methods in the Tick data structure
				JavaRDD<String> tick = rdd.map(x -> new String(x._2));
				JavaRDD<Row> rowRDD = tick.map(
					new Function<String, Row>() {
					public Row call(String record) throws Exception {
						if (record.trim().length() > 0) {
							Tick t = new Tick(record.trim());
							return RowFactory.create(new java.sql.Timestamp(t.getTimeInMillis()), t.getSymbolRoot(), t.getTradeVolume(), t.getTradePrice(), t.getSender(), t.getReceivers());
						}
						else {
							return RowFactory.create("", "", "", "", "", "");
						}
					}
				});

				// Apply our user-defined schema to the RDD and export it to a Hive table
				DataFrame batch_table = hiveContext.createDataFrame(rowRDD, schema);
//                master_tick_table.unionAll(tick_table);

				batch_table.registerTempTable("batchTable");
				hiveContext.sql("create table if not exists " + HIVE_TABLE + " as select * from batchTable");
				hiveContext.sql("insert into " + HIVE_TABLE + " select * from batchTable");

				// Print the schema that we will use for our Hive table
//                tick_table_for_zeppelin.printSchema();
				// Print the first 5 rows of our Hive table
//                tick_table_for_zeppelin.show(5);
				Row[] results = hiveContext.sql("SELECT count(*) from " + HIVE_TABLE).collect();
				System.out.println("Hive table size = " + results[0].toString().replaceAll("\\[(.*?)\\]", "$1"));
				// Save the Hive table so that it can be accessed by Zeppelin
				// This will be saved in a path like, /mapr/ian.cluster.com/user/hive/warehouse/
//                tick_table.write().mode(SaveMode.Append).saveAsTable("ticks");
//                tick_table.saveAsTable("ticks", SaveMode.Append);

			}
		});

		// TODO: test getting offsets from another topic, to use for fetching a subset of a topic
		ssc.start();
		ssc.awaitTermination();
	}
}
