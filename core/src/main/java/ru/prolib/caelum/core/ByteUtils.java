package ru.prolib.caelum.core;

public class ByteUtils {
	private static final ByteUtils instance = new ByteUtils();
	
	public static ByteUtils getInstance() {
		return instance;
	}
	
	public boolean isLongCompactTrade(long price, long volume) {
		return (0xFFFFFFFFFFFFFFC0L & volume) == 0L && (0xFFFFFFFFFFFF0000L & price) == 0L;
	}

	/**
	 * Convert long value to big endian byte array.
	 * <p>
	 * @param value - long value to convert
	 * @param result - byte array of 8 bytes to store the result
	 * @return number of significant bytes (at least 1)
	 */
	public int longToBytes(long value, byte result[]) {
		int num = 0, empty = (0x8000000000000000L & value) == 0 ? 0 : 0xFF;
		byte next_byte;
		for ( int i = 7; i >= 0; i -- ) {
			next_byte = (byte) (0xFF & value);
			result[i] = next_byte;
			if ( (0xFF & next_byte) != empty ) {
				num = 8 - i;
			}
			value >>= 8;
		}
		return num > 0 ? num : 1;
	}

	/**
	 * Extract long from big endian byte array.
	 * <p>
	 * @param bytes - source byte array
	 * @param offset - first byte starting offset inside the source
	 * @param num_bytes - number of bytes to read (maximum 8 bytes)
	 * @return extracted value
	 * @throws IllegalArgumentException num_bytes is greater than 8
	 */
	public long bytesToLong(byte bytes[], int offset, int num_bytes) {
		long result = ((bytes[offset] & 0x80) == 0) ? 0x0L : 0xFFFFFFFFFFFFFFFFL;
		int last = offset + num_bytes;
		for ( ; offset < last; offset ++ ) {
			result <<= 8;
			result |= (bytes[offset] & 0xFF);
		}
		return result;
	}
	
}
