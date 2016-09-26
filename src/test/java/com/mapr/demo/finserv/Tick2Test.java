package com.mapr.demo.finserv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import org.junit.Test;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class Tick2Test {

	public static final double N = 1e7;

	@Test
	public void testGetDate() throws IOException {
		List<String> data = Resources.readLines(Resources.getResource("sample-tick-01.txt"), Charsets.ISO_8859_1);
		Tick t = new Tick(data.get(0));
		assertEquals(t.getDate(), "080845201");

		ObjectMapper mapper = new ObjectMapper();
		System.out.printf("%s\n", mapper.writeValueAsString(t));
	}

	//@Test
	public void testSpeed() throws Exception {
		List<String> data = Resources.readLines(Resources.getResource("sample-tick-01.txt"), Charsets.ISO_8859_1);
		ObjectMapper mapper = new ObjectMapper();

		File tempFile = File.createTempFile("foo", "data");
		tempFile.deleteOnExit();
		System.out.printf("file = %s\n", tempFile);
		byte[] NEWLINE = "\n".getBytes();
		double t0 = System.nanoTime() * 1e-9;
		int m = data.size();
		try (OutputStream out = new BufferedOutputStream(new FileOutputStream(tempFile), 10_000_000)) {
			for (int i = 0; i < N; i++) {
				int j = i % m;
				Tick t = new Tick(data.get(j));
				out.write(mapper.writeValueAsBytes(t));
			}
		}
		long size = tempFile.length();
		double t = System.nanoTime() * 1e-9 - t0;
		System.out.printf("t = %.3f us, %.2f records/s, %.2f MB/s\n", t / N * 1e6, N / t, size / t / 1e6);
	}

	//@Test
	public void testBinarySpeed() throws Exception {
		List<String> data = Resources.readLines(Resources.getResource("sample-tick-01.txt"), Charsets.ISO_8859_1);

		double t0 = System.nanoTime() * 1e-9;
		File tempFile = File.createTempFile("foo", "data");
		tempFile.deleteOnExit();
		try (ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile), 10_000_000))) {
			for (int i = 0; i < N; i++) {
				int j = i % data.size();
				Tick t = new Tick(data.get(j));
				out.writeObject(t);
			}
		}
		double t = System.nanoTime() * 1e-9 - t0;
		System.out.printf("t = %.3f us, %.2f records/s\n", t / N * 1e6, N / t);
	}
}
