package ru.prolib.caelum.service;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class ItemDataResponse {
	private final long offset;
	private final String magic;
	
	public ItemDataResponse(long offset, String magic) {
		this.offset = offset;
		this.magic = magic;
	}
	
	/**
	 * Get last known/processed/reached offset.
	 * <p>
	 * @return offset
	 */
	public long getOffset() {
		return offset;
	}
	
	/**
	 * Get magic string.
	 * <p>
	 * @return checksum
	 */
	public String getMagic() {
		return magic;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(10095303, 9)
				.append(offset)
				.append(magic)
				.build();
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("offset", offset)
				.append("magic", magic)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != ItemDataResponse.class ) {
			return false;
		}
		ItemDataResponse o = (ItemDataResponse) other;
		return new EqualsBuilder()
				.append(o.offset, offset)
				.append(o.magic, magic)
				.build();
	}
	
}
