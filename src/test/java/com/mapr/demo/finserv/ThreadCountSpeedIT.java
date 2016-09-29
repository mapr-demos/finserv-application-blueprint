package com.mapr.demo.finserv;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.*;

/**
 * Tests the effect of threading on message transmission to lots of topics
 */
@RunWith(Parameterized.class)
public class ThreadCountSpeedIT {
	private static final String STREAM = "/mapr/my.cluster.com/user/mapr/taq";
	private static final double TIMEOUT = 30;  // seconds
	private static final int BATCH_SIZE = 1000000;  // The unit of measure for throughput is "batch size" per second
													// e.g. Throughput = X "millions of messages" per sec

	@BeforeClass
	public static void openDataFile() throws FileNotFoundException {
		data = new PrintWriter(new File("thread-count.csv"));
		data.printf("threadCount, topicCount, messageSize, i, t, rate, dt, batchRate\n");
	}

	@AfterClass
	public static void closeDataFile() {
		data.close();
	}

	private static PrintWriter data;
	
	@Parameterized.Parameters(name = "{index}: threads={0}, topics={1}")
	public static Iterable<Object[]> data() {
		return Arrays.asList(new Object[][]{
			{1, 50}, {2, 50}, {5, 50}, {10, 50},
			{1, 100}, {2, 100}, {5, 100}, {10, 100},
			{1, 200}, {2, 200}, {5, 200}, {10, 200},
			{1, 500}, {2, 500}, {5, 500}, {10, 500},
			{1, 1000}, {2, 1000}, {5, 1000}, {10, 1000}
		});
	}

	private int threadCount; // number of concurrent Kafka producers to run
	private int topicCount;  // number of Kafka topics in our stream
	private int messageSize = 100;  // size of each message sent into Kafka

	private static final ProducerRecord<String, byte[]> end = new ProducerRecord<>("end", null);

	public ThreadCountSpeedIT(int threadCount, int topicCount) {
		this.threadCount = threadCount;
		this.topicCount = topicCount;
	}

	private static class Sender extends Thread {
		private final KafkaProducer<String, byte[]> producer;
		private final BlockingQueue<ProducerRecord<String, byte[]>> queue;

		private Sender(KafkaProducer<String, byte[]> producer, BlockingQueue<ProducerRecord<String, byte[]>> queue) {
			this.producer = producer;
			this.queue = queue;
		}

		@Override
		public void run() {
			try {
				ProducerRecord<String, byte[]> rec = queue.take();
				while (rec != end) {
					// Here's were the sender thread sends a message.
					// Since we're not supplying a callback the send will be done asynchronously.
					// The outgoing message will go to a local buffer which is not necessarily FIFO,
					// but sending messages out-of-order does not matter since we're just trying to
					// test throughput in this class.
					producer.send(rec);
					rec = queue.take();
				}
			} catch (InterruptedException e) {
				System.out.printf("%s: Interrupted\n", this.getName());
			}
		}
	}

	@Test
	public void testThreads() throws Exception {
		System.out.printf("threadCount = %d, topicCount = %d\n", threadCount, topicCount);

		// Create new topic names. Kafka will automatically create these topics if they don't already exist.
		List<String> ourTopics = Lists.newArrayList();
		for (int i = 0; i < topicCount; i++) {
			// Topic names will look like, "t-00874"
			ourTopics.add(String.format("%s:t-%05d", STREAM, i));
		}

		// Create a message containing random bytes. We'll send this message over and over again
		// in our performance test, below.
		Random rand = new Random();
		byte[] buf = new byte[messageSize];
		rand.nextBytes(buf);
		Tick message = new Tick(buf);

		// Create a pool of sender threads.
		ExecutorService pool = Executors.newFixedThreadPool(threadCount);

		// We need some way to give each sender messages to publish.
		// We'll do that via this list of queues.
		List<BlockingQueue<ProducerRecord<String, byte[]>>> queues = Lists.newArrayList();
		for (int i = 0; i < threadCount; i++) {
			// We use BlockingQueue to buffer messages for each sender.
			// We use this type not for concurrency reasons (although it is thread safe) but
			// rather because it provides an efficient way for senders to take messages if
			// they're available and for us to generate those messages (see below).
			BlockingQueue<ProducerRecord<String, byte[]>> q = new ArrayBlockingQueue<>(1000);
			queues.add(q);
			// spawn each thread with a reference to "q", which we'll add messages to later.
			pool.submit(new Sender(getProducer(), q));
		}

		double t0 = System.nanoTime() * 1e-9;
		double batchStart = 0;

		// -------- Generate Messages for each Sender --------
		// Generate BATCH_SIZE messages at a time and send each one to a random sender thread.
		// The batch size was defined above as containing 1 million messages.
		// We want to send as many messages as possible until a timeout has been reached.
		// The timeout was defined above as 30 seconds.
		// We'll break out of this loop when that timeout occurs.
		for (int i = 0; i >= 0 && i < Integer.MAX_VALUE; ) {
			// Send each message in our batch (of 1 million messages) to a random topic.
			for (int j = 0; j < BATCH_SIZE; j++) {
				// Get a random topic (but always assign it to the same sender thread)
				String topic = ourTopics.get(rand.nextInt(topicCount));
				// The topic hashcode works in the sense that equal topics always have equal hashes.
				// So this will ensure that a topic will always be populated by the same sender thread.
				// We want to load balance senders without using round robin, because with round robin
				// all senders would have to send to all topics, and we've found that it's much faster
				// to minimize the number of topics each kafka producer sends to.
				// By using this hashcode we can maintain affinity between Kafka topic and sender thread.
				int qid = topic.hashCode() % threadCount;
				if (qid < 0) {
					qid += threadCount;
				}
				// Put a message to be published in the queue belonging to the sender we just selected.
				// That sender will automatically send this message as soon as possible.
				queues.get(qid).put(new ProducerRecord<>(topic, message.getData()));
			}
			i += BATCH_SIZE;
			double t = System.nanoTime() * 1e-9 - t0;
			double dt = t - batchStart;
			batchStart = t;
			// i = number of batches (number of "1 million messages" sent)
			// t = total elapsed time
			// i/t = throughput (number of batches sent overall per second)
			// dt = elapsed time for this batch
			// batch / dt = millions of messages sent per second for this batch
			data.printf("%d,%d,%d,%.3f,%.1f,%.3f,%.1f\n", threadCount, topicCount, i, t, i / t, dt, BATCH_SIZE / dt);
			if (t > TIMEOUT) {
				break;
			}
		}
		// We cleanly shutdown each producer thread by sending the predefined "end" message
		// then shutdown the threads in the pool after giving them a few seconds to see that
		// end message.
		for (int i = 0; i < threadCount; i++) {
			queues.get(i).add(end);
		}
		pool.shutdown();
		pool.awaitTermination(10, TimeUnit.SECONDS);
	}

	KafkaProducer<String, byte[]> getProducer() throws IOException {
		Properties props = new Properties();
		props.load(Resources.getResource("producer.props").openStream());
		// Properties reference:
		// https://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
		// props.put("batch.size", 16384);
		// props.put("linger.ms", 1);
		// props.put("buffer.memory", 33554432);

		return new KafkaProducer<>(props);
	}
}
