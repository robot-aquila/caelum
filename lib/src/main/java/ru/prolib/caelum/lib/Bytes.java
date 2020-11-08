package ru.prolib.caelum.lib;

import static ru.prolib.caelum.lib.ByteUtils.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Represents a segment of bytes.
 */
public class Bytes {
	private final byte[] source;
	private final int offset, length;
	
	public Bytes(byte[] source, int offset, int length) {
		this.source = source;
		this.offset = offset;
		this.length = length;
	}
	
	/**
	 * Get bytes source.
	 * <p>
	 * @return byte array
	 */
	public byte[] getSource() {
		return source;
	}
	
	/**
	 * Get index of the first byte of segment.
	 * <p>
	 * @return first byte offset
	 */
	public int getOffset() {
		return offset;
	}
	
	/**
	 * Get total number of bytes in this segment.
	 * <p>
	 * @return number of  bytes
	 */
	public int getLength() {
		return length;
	}
	
	/**
	 * Copy segment bytes to target byte array.
	 * <p>
	 * @param target - target array with enough number of bytes
	 * @return result array with segment bytes (same as target)
	 */
	public byte[] copyBytes(byte[] target) {
		System.arraycopy(source, offset, target, 0, length);
		return target;
	}
	
	/**
	 * Copy segment bytes to new array.
	 * <p>
	 * @return result array with segment bytes
	 */
	public byte[] copyBytes() {
		return copyBytes(new byte[length]);
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append(getClass().getSimpleName())
				.append("[source=").append(source)
				.append(" offset=").append(offset)
				.append(" length=").append(length)
				.append(" hex=").append(byteArrToHexString(source, offset, length))
				.append("]")
				.toString();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(70091507, 73)
				.append(copyBytes())
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != Bytes.class ) {
			return false;
		}
		Bytes o = (Bytes) other;
		if ( length != o.length ) {
			return false;
		}
		for ( int i = 0; i < length; i ++ ) {
			if ( source[offset + i] != o.source[o.offset + i] ) {
				return false;
			}
		}
		return true;
	}

}
