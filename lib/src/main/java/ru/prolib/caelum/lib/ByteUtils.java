package ru.prolib.caelum.lib;

import java.math.BigDecimal;

import org.apache.commons.lang3.StringUtils;

public class ByteUtils {
	private static final ByteUtils instance = new ByteUtils();
	
	public static ByteUtils getInstance() {
		return instance;
	}
	
	public boolean isLongCompact(long value, long volume) {
		return (0xFFFFFFFFFFFFFFC0L & volume) == 0L && (0xFFFFFFFFFFFF0000L & value) == 0L;
	}
	
	public boolean isNumberOfDecimalsFits4Bits(int decimals) {
		if ( decimals < 0 || decimals > 255 ) {
			throw new IllegalArgumentException("Number of decimals must be in range 0-255 but: " + decimals);
		}
		return decimals <= 15;
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
		if ( num == 0 ) {
			return 1;
		}
		if ( num == 8 ) {
			return 8;
		}
		int high_byte_index = 8 - num;
		if ( empty == 0 ) {
			// compact positive value shouldn't contain higher bit on
			if ( (result[high_byte_index] & 0x80) != 0 ) {
				num ++;
			}
		} else {
			// compact negative value shouldn't contain higher bit off
			if ( (result[high_byte_index] & 0x80) == 0 ) {
				num ++;
			}
		}
		return num;
	}

	/**
	 * Extract long from big-endian byte array.
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
	
	public long centsToLong(BigDecimal value) {
		return value.multiply(BigDecimal.TEN.pow(value.scale())).longValueExact();
	}
	
	public static String byteArrToHexString(byte[] source, int offset, int length) {
		String[] strings = new String[length];
		for ( int i = 0; i < length; i ++ ) {
			strings[i] = String.format("%02X", source[offset + i]);
		}
		return '{' + StringUtils.join(strings, ' ') + '}';
	}
	
	public static String byteArrToHexString(byte[] source) {
		return byteArrToHexString(source, 0, source.length);
	}
	
	public static String bytesToHexString(Bytes source) {
		return byteArrToHexString(source.getSource(), source.getOffset(), source.getLength());
	}
	
	/**
	 * Convert hexadecimal string to byte array.
	 * <p>
	 * @param hex - string of bytes in hex format. Allowed format examples:
	 * <ul>
	 * <li>{ FE AB 12 00 }</li>
	 * <li>FE AB 12 00</li>
	 * <li>FEAB1200</li>
	 * <li>  F  E AB  1 20 0 - actually is wrong format but will be eaten and converted to FEAB1200</li>
	 * </ul> 
	 * @return array of bytes
	 */
	public static byte[] hexStringToByteArr(String hex) {
		hex = StringUtils.remove(hex, ' ');
		hex = StringUtils.removeStart(hex, "{");
		hex = StringUtils.removeEnd(hex, "}");
		int count = hex.length() / 2, corrector = 0;
		if ( hex.length() % 2 > 0 ) {
			count ++;
			corrector = -1;
		}
		byte[] bytes = new byte[count];
		for ( int i = 0; i < count; i ++ ) {
			int offset = i * 2 + corrector;
			char first = offset < 0 ? '0' : hex.charAt(offset), second = hex.charAt(offset + 1);
			String x = String.valueOf(first) + second;
			bytes[i] = (byte) Integer.parseInt(x, 16);
		}
		return bytes;
	}
	
	public static Bytes hexStringToBytes(String hex) {
		byte[] bytes = hexStringToByteArr(hex);
		return new Bytes(bytes, 0, bytes.length);
	}
}
