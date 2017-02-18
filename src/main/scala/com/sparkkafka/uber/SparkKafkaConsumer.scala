package com.sparkkafka.uber

import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.kafka09.{ ConsumerStrategies, KafkaUtils, LocationStrategies }
import org.apache.spark.streaming.dstream.DStream

object SparkKafkaConsumer {

  case class Tick(date: Long, exchange: String, symbol: String, price: Double, volume: Double, sender: String, receivers: Array[String]) extends Serializable

  def parseTick(record: String): Tick = {
    val tick = new com.mapr.demo.finserv.Tick(record)
    val receivers: Array[String]  = (List(tick.getReceivers) map (_.toString)).toArray
    Tick(tick.getTimeInMillis, tick.getExchange, tick.getSymbolRoot, tick.getTradePrice, tick.getTradeVolume, tick.getSender, receivers)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      throw new IllegalArgumentException("You must specify the subscribe topic. For example /user/mapr/taq:trades")
    }

    val Array(topics) = args

    val brokers = "maprdemo:9092" // not needed for MapR Streams, needed for Kafka
    val groupId = "sparkApplication"
    val batchInterval = "2"
    val pollTimeout = "10000"

    val sparkConf = new SparkConf().setAppName("TickStream")

    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval.toInt))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
      "spark.kafka.poll.time" -> pollTimeout,
      "spark.streaming.kafka.consumer.poll.ms" -> "8192"
    )

    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    val messagesDStream = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, consumerStrategy
    )
    // get message values from key,value
    val valuesDStream: DStream[String] = messagesDStream.map(_.value())

    valuesDStream.foreachRDD { rdd =>

      // There exists at least one element in RDD
      if (!rdd.isEmpty) {
        val count = rdd.count
        println("count received " + count)
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        import org.apache.spark.sql.functions._
        import org.apache.spark.sql.types._

        val df = rdd.map(parseTick).toDF()
        // Display the top 20 rows of DataFrame
        println("tick data")
        df.show()

        // TODO: persist a temporary view to Hive

//        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
//        import spark.implicits._
//
//        import org.apache.spark.sql.functions._
//        val df = spark.read.schema(schema).json(rdd)
//        df.show
//        df.createOrReplaceTempView("uber")
//
//        df.groupBy("cluster").count().show()
//
//        spark.sql("select cluster, count(cluster) as count from uber group by cluster").show
//
//        spark.sql("SELECT hour(uber.dt) as hr,count(cluster) as ct FROM uber group By hour(uber.dt)").show
//
//
//        df.groupBy("cluster").count().show()
//
//        val countsDF = df.groupBy($"cluster", window($"dt", "1 hour")).count()
//        countsDF.createOrReplaceTempView("uber_counts")
//
//        spark.sql("select cluster, sum(count) as total_count from uber_counts group by cluster").show
//        //spark.sql("sql select cluster, date_format(window.end, "MMM-dd HH:mm") as dt, count from uber_counts order by dt, cluster").show
//
//        spark.sql("select cluster, count(cluster) as count from uber group by cluster").show
//
//        spark.sql("SELECT hour(uber.dt) as hr,count(cluster) as ct FROM uber group By hour(uber.dt)").show
      }
    }

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
