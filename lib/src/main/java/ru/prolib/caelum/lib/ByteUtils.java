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
	 * Convert long value to big-endian byte array.
	 * <p>
	 * @param value - long value to convert
	 * @param result - byte array of 8 bytes to store the result. Keep in mind that significant
	 * data will be written to the end of array. So if there is N significant bytes where N is
	 * less than than 8 then N last bytes of the array will store that data and the last byte
	 * is less significant byte.
	 * @return number of significant bytes (at least 1, at most 8)
	 */
	public int longToByteArray(long value, byte result[]) {
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
	
	public Bytes longToBytes(long value) {
		byte bytes[] = new byte[8];
		int num = longToByteArray(value, bytes);
		return new Bytes(bytes, 8 - num, num);
	}
	
	/**
	 * Convert integer value to big-endian byte array.
	 * <p>
	 * @param value - integer value to convert
	 * @param result - byte array of 4 bytes to store the result. Keep in mind that significant
	 * data will be written to the end of array. So if there is N significant bytes where N is
	 * less than than 4 then N last bytes of the array will store that data and the last byte
	 * is less significant byte.
	 * @return number of significant bytes (at least 1, at most 4)
	 */
	public int intToByteArray(int value, byte result[]) {
		int num = 0, empty = (0x80000000 & value) == 0 ? 0 : 0xFF;
		byte next_byte;
		for ( int i = 3; i >= 0; i -- ) {
			next_byte = (byte) (0xFF & value);
			result[i] = next_byte;
			if ( (0xFF & next_byte) != empty ) {
				num = 4 - i;
			}
			value >>= 8;
		}
		if ( num == 0 ) {
			return 1;
		}
		if ( num == 4 ) {
			return 4;
		}
		int high_byte_index = 4 - num;
		if ( empty == 0 ) {
			if ( (result[high_byte_index] & 0x80) != 0 ) {
				num ++;
			}
		} else {
			if ( (result[high_byte_index] & 0x80) == 0 ) {
				num ++;
			}
		}
		return num;
	}
	
	/**
	 * Get number of significant bytes of an integer value.
	 * <p>
	 * Actually this is a fast size calculation to store integers like methods
	 * {@link #intToByteArray(int, byte[])} and {@link #intToBytes(int)} do.
	 * <p>
	 * @param value - value to test
	 * @return number of bytes needed to store significant data
	 */
	public int intSize(int value) {
	    if ( (0x80000000 & value) == 0 ) {
	        if ( (0xFFFFFF80 & value) == 0 ) return 1;
	        if ( (0xFFFF8000 & value) == 0 ) return 2;
	        if ( (0xFF800000 & value) == 0 ) return 3;
	    } else {
	        if ( (0xFFFFFF80 & value) == 0xFFFFFF80) return 1;
	        if ( (0xFFFF8000 & value) == 0xFFFF8000) return 2;
	        if ( (0xFF800000 & value) == 0xFF800000) return 3;
	    }
        return 4;
	}
	
	public Bytes intToBytes(int value) {
		byte bytes[] = new byte[4];
		int num = intToByteArray(value, bytes);
		return new Bytes(bytes, 4 - num, num);
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
	
	/**
	 * Pack integer into 3 bit field.
	 * <p>
	 * @param source - source byte to apply changes
	 * @param value - value to pack
	 * @param position - position of the field inside the result byte. Allowed range
     * is between 0 (rightmost position without an offset) and 5 (most possible offset for 3 bit field).
     * @return packed value
     * @throws IllegalArgumentException - value is out of range 0-7 or position out of range 0-5
	 */
	public byte intToF3b(byte source, int value, int position) {
        if ( (value & 0xFFFFFFF8) != 0 ) {
            throw new IllegalArgumentException("Value out of range 0-7: " + value);
        }
        if ( position < 0 || position > 5 ) {
            throw new IllegalArgumentException("Position out of range 0-5: " + position);
        }
        byte mask = (byte)(0b00000111 << position);
        return (byte) ((source | mask) ^ mask | (byte)(value << position));
	}
    
	/**
	 * Pack integer into 3 bit field.
	 * <p>
	 * @param value - value to pack
	 * @param position - position of the field inside the result byte. Allowed range
	 * is between 0 (rightmost position without an offset) and 5 (most possible offset for 3 bit field).
	 * @return packed value
	 * @throws IllegalArgumentException - value is out of range 0-7 or position out of range 0-5
	 */
    public byte intToF3b(int value, int position) {
        if ( (value & 0xFFFFFFF8) != 0 ) {
            throw new IllegalArgumentException("Value out of range 0-7: " + value);
        }
        if ( position < 0 || position > 5 ) {
            throw new IllegalArgumentException("Position out of range 0-5: " + position);
        }
        return value == 0 ? 0 : (byte)(value << position);
    }
    
    /**
     * Unpack integer from 3 bit field.
     * <p>
     * @param source - packed value source
     * @param position - position of the field inside the result byte. Allowed range
     * is between 0 (rightmost position without an offset) and 5 (most possible offset for 3 bit field).
     * @return unpacked value
     * @throws IllegalArgumentException - position out of range 0-5
     */
    public int f3bToInt(byte source, int position) {
        switch ( position ) {
        case 0: return (source & 0b00000111);
        case 1: return (source & 0b00001110) >> 1;
        case 2: return (source & 0b00011100) >> 2;
        case 3: return (source & 0b00111000) >> 3;
        case 4: return (source & 0b01110000) >> 4;
        case 5: return (source & 0b11100000) >> 5;
        default: throw new IllegalArgumentException("Position out of range 0-5: " + position);
        }
    }
    
    /**
     * Pack boolean value to 1 bit field.
     * <p>
     * @param source - source byte to apply changes
     * @param value - value to pack
     * @param position - position of the field inside the result byte. Allowed range
     * is between 0 (rightmost position without an offset) and 7 (most possible offset for 1 bit field).
     * @return packed value
     * @throws IllegalArgumentException - position out of range 0-7
     */
    public byte boolToBit(byte source, boolean value, int position) {
        if ( (position & 0xFFFFFFF8) != 0 ) {
            throw new IllegalArgumentException("Position out of range 0-7: " + position);
        }
        byte mask = (byte)(1 << position);
        if ( value ) {
            return (source & mask) == 0 ? (byte)(source | mask) : source;
        } else {
            return (source & mask) == 0 ? source : (byte)(source ^ mask);
        }
    }
    
    /**
     * Pack boolean value to 1 bit field.
     * <p>
     * @param value - value to pack
     * @param position - position of the field inside the result byte. Allowed range
     * is between 0 (rightmost position without an offset) and 7 (most possible offset for 1 bit field).
     * @return packed value
     * @throws IllegalArgumentException - position out of range 0-7
     */
    public byte boolToBit(boolean value, int position) {
        if ( (position & 0xFFFFFFF8) != 0 ) {
            throw new IllegalArgumentException("Position out of range 0-7: " + position);
        }
        return value ? (byte) (1 << position) : 0;
    }
    
    /**
     * Unpack boolean value from 1 bit field.
     * <p>
     * @param source - packed value source
     * @param position - position of the field inside the result byte. Allowed range
     * is between 0 (rightmost position without an offset) and 7 (most possible offset for 1 bit field).
     * @return unpacked value
     * @throws IllegalArgumentException - position out of range 0-7
     */
    public boolean bitToBool(byte source, int position) {
        if ( (position & 0xFFFFFFF8) != 0 ) {
            throw new IllegalArgumentException("Position out of range 0-7: " + position);
        }
        return (source & (1 << position)) != 0;
    }
    
    /**
     * Pack size between 1 and 8 to 3-bit field.
     * <p>
     * Such fields are used to store length of size fields.
     * Despite 2 bits are enough to pack size of integer there it is 3 bit field
     * because those fields can be alternatively used for other purposes.
     * <p>
     * @param size - size to pack. Allowed range is between 1 and 8.
     * @param position - position of the 3-bit field inside the result byte.
     * See {@link #intToF3b(int, int)} for details.
     * @return packed 3-bit field with value size in bytes
     * @throws IllegalArgumentException - size out of range 1-8
     */
    public byte sizeToF3b(int size, int position) {
        if ( size < 1 || size > 8 ) {
            throw new IllegalArgumentException("Size out of range 1-8: " + size);
        }
        return intToF3b(size - 1, position);
    }
    
    /**
     * Unpack size from 3-bit field.
     * <p>
     * @param source - source byte to take data of
     * @param position - position of the 3-bit field inside the result byte.
     * See {@link #f3bToInt(byte, int)} for details.
     * @return number of bytes that are represent significant bytes of an integer
     */
    public int f3bToSize(byte source, int position) {
        return f3bToInt(source, position) + 1;
    }

}
