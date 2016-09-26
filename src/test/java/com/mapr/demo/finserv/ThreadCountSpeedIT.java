package com.mapr.demo.finserv;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.*;
import org.junit.Test;

/**
 * Tests the effect of threading on message transmission to lots of topics
 */
@RunWith(Parameterized.class)
public class ThreadCountSpeedIT {

	private static PrintWriter data;

	private final double timeout = 30;
	private final int batch = 1000000;
	private final int threadCount;
	private final int topicCount;
	private final int messageSize = 100;
	private final int batchSize = 0;

	private static final ProducerRecord<String, byte[]> END = new ProducerRecord<>("end", null);

	@BeforeClass
	public static void openDataFile() throws FileNotFoundException {
		data = new PrintWriter(new File("thread-count.csv"));
		data.printf("threadCount, topicCount, messageSize, i, t, rate, dt, batchRate\n");
	}

	@AfterClass
	public static void closeDataFile() {
		data.close();
	}

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

	public ThreadCountSpeedIT(int threadCount, int topicCount) {
		this.threadCount = threadCount;
		this.topicCount = topicCount;
	}

	private static class Sender extends Thread {

		private final KafkaProducer<String, byte[]> producer;
		private final BlockingQueue<ProducerRecord<String, byte[]>> queue;

		public Sender(KafkaProducer<String, byte[]> producer, BlockingQueue<ProducerRecord<String, byte[]>> queue) {
			this.producer = producer;
			this.queue = queue;
		}

		@Override
		public void run() {
			try {
				ProducerRecord<String, byte[]> rec = queue.take();
				while (rec != END) {
					producer.send(rec);
					rec = queue.take();
				}
			}
			catch (InterruptedException e) {
				System.out.printf("%s: Interrupted\n", this.getName());
			}
		}
	}

	@Test
	public void testThreads() throws Exception {
		System.out.printf("threadCount = %d, topicCount = %d\n", threadCount, topicCount);

		String stream = "/mapr/se1/user/tdunning/taq";
		List<String> ourTopics = Lists.newArrayList();
		for (int i = 0; i < topicCount; i++) {
			ourTopics.add(String.format("%s:t-%05d", stream, i));
		}
		Random rand = new Random();

		byte[] buf = new byte[messageSize];
		rand.nextBytes(buf);
		Tick message = new Tick(buf);

		ExecutorService pool = Executors.newFixedThreadPool(threadCount);
		List<BlockingQueue<ProducerRecord<String, byte[]>>> queues = Lists.newArrayList();
		for (int i = 0; i < threadCount; i++) {
			BlockingQueue<ProducerRecord<String, byte[]>> q = new ArrayBlockingQueue<>(1000);
			queues.add(q);
			pool.submit(new Sender(getProducer(), q));
		}

		double t0 = System.nanoTime() * 1e-9;
		double batchStart = 0;

		for (int i = 0; i < 1e9;) {
			for (int j = 0; j < batch; j++) {
				String topic = ourTopics.get(rand.nextInt(topicCount));
				int qid = topic.hashCode() % threadCount;
				if (qid < 0) {
					qid += threadCount;
				}
				queues.get(qid).put(new ProducerRecord<>(topic, message.getData()));
			}
			i += batch;
			double t = System.nanoTime() * 1e-9 - t0;
			double dt = t - batchStart;
			batchStart = t;
			data.printf("%d,%d,%d,%.3f,%.1f,%.3f,%.1f\n", threadCount, topicCount, i, t, i / t, dt, batch / dt);
			if (t > timeout) {
				break;
			}
		}
		for (int i = 0; i < threadCount; i++) {
			queues.get(i).add(END);
		}
		pool.shutdown();
		pool.awaitTermination(10, TimeUnit.SECONDS);
	}

	KafkaProducer<String, byte[]> getProducer() throws IOException {
		Properties p = new Properties();
		p.load(Resources.getResource("producer.props").openStream());

		if (batchSize > 0) {
			p.setProperty("batch.size", String.valueOf(batchSize));
		}
		return new KafkaProducer<>(p);
	}
}
