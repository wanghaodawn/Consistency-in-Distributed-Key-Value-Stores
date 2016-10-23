/*
 * This is the data structure that is used to store data in data centers
 */
public class DataUnit {
	private String value;
	private long timestamp;

	public DataUnit(String value, long timestamp) {
		value = value;
		timestamp = timestamp;
	}

	public String getValue() {
		return value;
	}

	public long getTimestamp() {
		return timestamp;
	}
}