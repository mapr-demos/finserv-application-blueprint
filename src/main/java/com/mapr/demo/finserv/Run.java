package com.mapr.demo.finserv;

import com.google.common.base.Preconditions;
import static com.mapr.demo.finserv.Producer.configureProducer;
import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pick whether we want to run as producer or consumer. This lets us have a single executable as a build target.
 */
public class Run {

	private static final Logger LOG = LoggerFactory.getLogger(Run.class);

	public static void main(String[] args) throws IOException, Exception {
		final String message = "USAGE:\n"
				+ "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run producer [source data file] [stream:topic]\n"
				+ "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run consumer [stream:topic]\n";
		Preconditions.checkArgument(args.length > 1, message);

		switch (args[0]) {
			case "producer":
				Producer producer = getProducer(args);
				producer.produce();
				break;
			case "consumer":
				Consumer consumer = getConsumer(args);
				consumer.consume();
				break;
			default:
				throw new IllegalArgumentException("Don't know how to do " + args[0]);
		}
		System.exit(0);
	}

	private static Producer getProducer(final String args[]) {
		final String message = "ERROR: You must specify the input data file and stream:topic.\n" +
			"USAGE:\n\tjava -cp `mapr classpath` -jar producer [source file | source directory] [stream:topic]\n" + 
			"Example:\n\tjava -cp `mapr classpath` -jar producer data/taqtrade20131218 /usr/mapr/taq:trades";
		Preconditions.checkArgument(args.length == 3, message);

		String topic = args[2];
		LOG.debug("Publishing to topic: {}", topic);

		configureProducer();
		File directory = new File(args[1]);
		return new Producer(topic, directory);
	}

	private static Consumer getConsumer(final String args[]) {
		final String message = "ERROR: You must specify a stream:topic to consume data from.\n" +
			"USAGE:\n\tjava -cp `mapr classpath` -jar consumer [stream:topic] [NUM_THREADS] [verbose]\n" + 
			"Example:\n\tjava -cp `mapr classpath` -jar consumer /usr/mapr/taq:trades 2 verbose";
		Preconditions.checkArgument(args.length > 2 && args.length < 5, message);

		String topic = args[1];
		LOG.debug("Subscribed to : {}", topic);
		
		boolean verbose = "verbose".equals(args[args.length - 1]);

		int threadCount = 1;
		if (args.length == 4) {
			threadCount = Integer.valueOf(args[2]);
		}
		if (args.length == 3 && !"verbose".equals(args[2])) {
			threadCount = Integer.valueOf(args[2]);
		}

		return new Consumer(topic, verbose, threadCount);
	}
}
