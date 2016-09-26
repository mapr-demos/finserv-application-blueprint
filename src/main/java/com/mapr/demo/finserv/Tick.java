package com.mapr.demo.finserv;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * This tick is a data structure containing a single tick that avoids parsing the underlying bytes as long as possible.
 * <p>
 * By using annotations, it also supports fast serialization to JSON.
 */
public class Tick implements Serializable {

	private byte[] data;

	public Tick(byte[] data) {
		this.data = data;
	}

	public Tick(String data) {
		this.data = data.getBytes(Charsets.ISO_8859_1);
	}

	public byte[] getData() {
		return this.data;
	}

	@JsonProperty("date")
	public String getDate() {
		return new String(data, 0, 9);
	}

	public long getTimeInMillis() {
		// NYSE TAQ records do not reference year, month, day. So, we'll hard code, for now.
		Calendar timestamp = new GregorianCalendar(2013, 12, 1);
		timestamp.set(Calendar.HOUR, Integer.valueOf(new String(data, 0, 2)));
		timestamp.set(Calendar.MINUTE, Integer.valueOf(new String(data, 2, 2)));
		timestamp.set(Calendar.SECOND, Integer.valueOf(new String(data, 4, 2)));
		timestamp.set(Calendar.MILLISECOND, Integer.valueOf(new String(data, 6, 3)));
		return timestamp.getTimeInMillis();
	}

	@JsonProperty("exchange")
	public String getExchange() {
		return new String(data, 9, 1);
	}

	@JsonProperty("symbol-root")
	public String getSymbolRoot() {
		return trim(10, 6);
	}

	@JsonProperty("symbol-suffix")
	public String getSymbolSuffix() {
		return trim(16, 10);
	}

	@JsonProperty("sale-condition")
	public String getSaleCondition() {
		return trim(26, 4);
	}

	@JsonProperty("trade-volume")
	public double getTradeVolume() {
		return digitsAsInt(30, 9);
	}

	@JsonProperty("trade-price")
	public double getTradePrice() {
		return digitsAsDouble(39, 11, 4);
	}

	@JsonProperty("trade-correction-indicator")
	public String getTradeCorrectionIndicator() {
		return new String(data, 51, 2);
	}

	@JsonProperty("trade-sequence-number")
	public String getTradeSequenceNumber() {
		return new String(data, 53, 16);
	}

	@JsonProperty("trade-source")
	public String getTradeSource() {
		return new String(data, 69, 1);
	}

	@JsonProperty("trade-reporting-facility")
	public String getTradeReportingFacility() {
		return new String(data, 70, 1);
	}

	@JsonProperty("sender")
	public String getSender() {
		return new String(data, 71, 4);
	}

	@JsonProperty("receiver-list")
	public List<String> getReceivers() {
		List<String> receivers = new ArrayList<>();
		for (int i = 0; data.length >= 79 + i * 4; i++) {
			receivers.add(new String(data, 75 + i * 4, 4));
		}
		return receivers;
	}

	private double digitsAsDouble(int start, int length, int decimals) {
		double r = digitsAsInt(start, length);
		for (int i = 0; i < decimals; i++) {
			r = r / 10;
		}
		return r;
	}

	private int digitsAsInt(int start, int length) {
		int r = 0;
		for (int i = start; i < start + length; i++) {
			if (data[i] != ' ') {
				r = r * 10 + data[i] - '0';
			}
		}
		return r;
	}

	private String trim(int start, int length) {
		int i = start;
		int j = start + length;
		while (i < start + length && data[i] == ' ') {
			i++;
		}
		while ((j - i) > 0 && data[j] == ' ') {
			j--;
		}
		return new String(data, i, j - i + 1);
	}

	public void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.writeInt(data.length);
		out.write(data);
	}

	public void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		int length = in.readInt();
		data = new byte[length];
		int n = in.read(data);
		if (n != length) {
			throw new IOException("Couldn't read entire Tick object, only got " + n + " bytes");
		}
	}
}
