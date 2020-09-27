package ru.prolib.caelum.backnode.mvc;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import ru.prolib.caelum.backnode.ValueFormatter;
import ru.prolib.caelum.lib.IItem;
import ru.prolib.caelum.lib.ItemType;

public class ItemMvcAdapter {
	private final ValueFormatter formatter;
	private final IItem item;

	public ItemMvcAdapter(IItem item, ValueFormatter formatter) {
		this.formatter = formatter;
		this.item = item;
	}
	
	public ItemMvcAdapter(IItem item) {
		this(item, ValueFormatter.getInstance());
	}
	
	public IItem getItem() {
		return item;
	}
	
	public String getSymbol() {
		return item.getSymbol();
	}
	
	public String getTime() {
		return item.getTimeAsInstant().toString();
	}
	
	public long getTimeMillis() {
		return item.getTime();
	}
	
	public long getOffset() {
		return item.getOffset();
	}
	
	public ItemType getType() {
		return item.getType();
	}
	
	public String getValue() {
		return formatter.format(item.getValue(), item.getDecimals());
	}
	
	public String getVolume() {
		return formatter.format(item.getVolume(), item.getVolumeDecimals());
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != ItemMvcAdapter.class ) {
			return false;
		}
		ItemMvcAdapter o = (ItemMvcAdapter) other;
		return new EqualsBuilder()
				.append(o.item, item)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(172076541, 703)
				.append(item)
				.build();
	}
	
}
