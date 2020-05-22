package ru.prolib.caelum.core;

public class ByteUtils {

	/**
	 * Convert long value to big endian byte array.
	 * <p>
	 * @param value - long value to convert
	 * @param result - byte array of 8 bytes to store the result
	 * @return number of significant bytes (at least 1)
	 */
	public int longToBytes(long value, byte result[]) {
		int num = 0;
		for ( int i = 7; i >= 0; i -- ) {
			if ( (result[i] = (byte) (0xFF & value)) != 0 ) {
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
		long result = 0L;
		int last = offset + num_bytes;
		for ( ; offset < last; offset ++ ) {
			result <<= 8;
			result |= (bytes[offset] & 0xFF);
		}
		return result;
	}
	
}
