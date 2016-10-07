/*
 * Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved
 */
package com.mapr.demo.finserv;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {

	private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
	private static final long POLL_INTERVAL = 4000;  // consumer poll every X milliseconds
	private static final long OFFSET_INTERVAL = 10000;  // record offset once every X messages
	private static final ProducerRecord<String, byte[]> END = new ProducerRecord<>("end", null);
	private static final AtomicLong COUNT = new AtomicLong();
	private final boolean verbose;
	private final String topic;
	private int threadCount = 1;
	private KafkaConsumer consumer;
	private final int batchSize = 0;
	private long newcount = 0;
	private long oldcount = 0;

	public Consumer(final String topic, final boolean verbose, final int threadCount) {
		this.topic = topic;
		this.verbose = verbose;
		this.threadCount = threadCount;
	}

	private static class Sender extends Thread {
		private final KafkaProducer<String, byte[]> producer;
		private final KafkaProducer<String, String> offset_producer;
		private final BlockingQueue<ProducerRecord<String, byte[]>> queue;
		public Sender(KafkaProducer<String, byte[]> producer, KafkaProducer<String, String> offset_producer, BlockingQueue<ProducerRecord<String, byte[]>> queue) {
			this.producer = producer;
			this.offset_producer = offset_producer;
			this.queue = queue;
		}

		@Override
		public void run() {
			try {
				ProducerRecord<String, byte[]> rec = queue.take();
				while (rec != END) {
					final ProducerRecord<String, byte[]> rec_backup = rec;  // if send fails, add this back to the queue
					COUNT.incrementAndGet();
					// Record an offset every once in a while
					if (COUNT.get() % OFFSET_INTERVAL != 0) {
						producer.send(rec, (RecordMetadata metadata, Exception e) -> {
							if (metadata == null || e != null) {
								// If there appears to have been an error, decrement our counter metric
								COUNT.decrementAndGet();
								queue.add(rec_backup);
							}
						});
					}
					else {
						Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
						String event_timestamp = Long.toString(cal.getTimeInMillis());
						producer.send(rec, (RecordMetadata metadata, Exception e) -> {
							if (metadata == null || e != null) {
								// If there appears to have been an error, decrement our counter metric
								COUNT.decrementAndGet();
								queue.add(rec_backup);
							}
							else {
								offset_producer.send(new ProducerRecord<>(metadata.topic() + "-offset", event_timestamp, Long.toString(metadata.offset())));
							}
						});
					}
					rec = queue.take();
				}
			}
			catch (InterruptedException e) {
				System.out.printf("%s: Interrupted\n", this.getName());
			}
		}
	}

	/**
	 * Fan out messages from raw tick stream to topics belonging to the traders (sender and receivers)
	 * participating in each tick. This method is a consumer of raw data, but a producer
	 * for each participant topic.
	 */
	public void consume() throws Exception {
		System.out.println("Spawning " + threadCount + " consumer threads");

		// Create a pool of sender threads.
		ExecutorService pool = Executors.newFixedThreadPool(threadCount);

		// We need some way to give each sender messages to publish.
		// We'll do that via this list of queues.
		List<BlockingQueue<ProducerRecord<String, byte[]>>> queues = Lists.newArrayList();

		for (int i = 0; i < threadCount; i++) {
			/* We use BlockingQueue to buffer messages for each sender.
			 * We use this type not for concurrency reasons (although it is thread safe) but
			 * rather because it provides an efficient way for sender threads to take messages
			 * if they're available and for us to generate those messages (see below).
			 */
			BlockingQueue<ProducerRecord<String, byte[]>> q = new ArrayBlockingQueue<>(1000);
			queues.add(q);

			// spawn each thread with a reference to "q", which we'll add messages to later.
			pool.submit(new Sender(getProducer(), getOffsetProducer(), q));
		}

		configureConsumer();
		List<String> topics = new ArrayList<>();
		topics.add(topic);
		consumer.subscribe(topics);

		int i = 0;
		double t0 = System.nanoTime() * 1e-9;
		double t = t0;
		while (true) {
			i++;
			// Request unread messages from the topic.
			ConsumerRecords<String, byte[]> records;
			records = consumer.poll(POLL_INTERVAL);
			if (records == null || records.count() == 0) {
				if (COUNT.get() >= 10) {
					System.out.println("The " + topic + "topic is empty. Exiting...");
					pool.shutdown();
					pool.awaitTermination(10, TimeUnit.SECONDS);
					System.exit(0);
					break;
				}
				continue;
			}

			try {
				for (ConsumerRecord<String, byte[]> raw_record : records) {
					String key = Long.toString(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTimeInMillis());
					byte[] data = raw_record.value();
					String sender_id = new String(data, 71, 4);
					String send_topic = "/user/mapr/taq:sender_" + sender_id;

					/* ENSURE TOPIC AFFINITY FOR EACH THREAD:
					 * The topic hashcode works in the sense that equal topics always have equal hashes.
					 * So this will ensure that a topic will always be populated by the same sender thread.
					 * We want to load balance senders without using round robin, because with round robin
					 * all senders would have to send to all topics, and we've found that it's much faster
					 * to minimize the number of topics each kafka producer sends to.
					 * By using this hashcode we can maintain affinity between Kafka topic and sender thread.
					 */
					int qid = send_topic.hashCode() % threadCount;
					if (qid < 0) {
						qid += threadCount;
					}

					/* Put a message to be published in the queue belonging to the sender we just selected.
					 * That sender will automatically send this message as soon as possible.
					 */
					queues.get(qid).put(new ProducerRecord<>(send_topic, key, data));
					for (int j = 0; (79 + j * 4) <= data.length; j++) {
						String receiver_id = new String(data, 75 + j * 4, 4);
						String recv_topic = "/user/mapr/taq:receiver_" + receiver_id;
						qid = recv_topic.hashCode() % threadCount;
						if (qid < 0) {
							qid += threadCount;
						}
						queues.get(qid).put(new ProducerRecord<>(recv_topic, key, data));
					}
				}
			}
			catch (StringIndexOutOfBoundsException e) {
				System.err.println("Invalid record");
			}
			double dt = System.nanoTime() * 1e-9 - t;

			if (dt > 1) {
				newcount = COUNT.get() - oldcount;
				System.out.printf("Total sent: %d, %.02f Kmsgs/sec\n", COUNT.get(), newcount / (System.nanoTime() * 1e-9 - t0) / 1000);
				t = System.nanoTime() * 1e-9;
				oldcount = newcount;
			}
		}
	}

	/**
	 * Set the configuration parameters for the producer to sender/receiver topics.
	 */
	private KafkaProducer<String, byte[]> getProducer() throws IOException {
		Properties p = new Properties();
		p.load(Resources.getResource("producer.props").openStream());

		if (batchSize > 0) {
			p.setProperty("batch.size", String.valueOf(batchSize));
		}

		return new KafkaProducer<>(p);
	}

	/**
	 * Set the configuration parameters for the producer to sender/receiver_offset topics.
	 */
	private KafkaProducer<String, String> getOffsetProducer() throws IOException {
		Properties p = new Properties();
		p.load(Resources.getResource("offset_producer.props").openStream());

		if (batchSize > 0) {
			p.setProperty("batch.size", String.valueOf(batchSize));
		}

		return new KafkaProducer<>(p);
	}

	/**
	 * Set the configuration parameters for the raw tick topic consumer.
	 */
	private void configureConsumer() {
		Properties props = new Properties();
		props.put("group.id", "group-" + new Random().nextInt(100000));
		props.put("enable.auto.commit", "true");
		props.put("group.id", "mapr-workshop");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		consumer = new KafkaConsumer<>(props);
	}
}
