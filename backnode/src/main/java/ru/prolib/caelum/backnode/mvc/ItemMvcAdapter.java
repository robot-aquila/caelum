package ru.prolib.caelum.backnode.mvc;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import ru.prolib.caelum.backnode.ValueFormatter;
import ru.prolib.caelum.core.ItemType;
import ru.prolib.caelum.itemdb.IItemData;

public class ItemMvcAdapter {
	private final ValueFormatter formatter;
	private final IItemData itemData;

	public ItemMvcAdapter(IItemData item_data, ValueFormatter formatter) {
		this.formatter = formatter;
		this.itemData = item_data;
	}
	
	public ItemMvcAdapter(IItemData item_data) {
		this(item_data, ValueFormatter.getInstance());
	}
	
	public IItemData getItemData() {
		return itemData;
	}
	
	public String getSymbol() {
		return itemData.getSymbol();
	}
	
	public String getTime() {
		return itemData.getTimeAsInstant().toString();
	}
	
	public long getTimeMillis() {
		return itemData.getTime();
	}
	
	public long getOffset() {
		return itemData.getOffset();
	}
	
	public ItemType getType() {
		return itemData.getItem().getType();
	}
	
	public String getValue() {
		return formatter.format(itemData.getItem().getValue(), itemData.getItem().getDecimals());
	}
	
	public String getVolume() {
		return formatter.format(itemData.getItem().getVolume(), itemData.getItem().getVolumeDecimals());
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
				.append(o.itemData, itemData)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(172076541, 703)
				.append(itemData)
				.build();
	}
	
}
