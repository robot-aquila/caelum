package ru.prolib.caelum.symboldb.fdb;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.tuple.Pair;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.symboldb.CategorySymbol;
import ru.prolib.caelum.symboldb.EventKey;

public class FDBSchema {
	private static final byte[] trueBytes =  Tuple.from(true).pack();
			
	private final Subspace
		spRoot,
		spCategory,
		spCategorySymbol,
		spEvents;
	
	public FDBSchema(Subspace root) {
		spRoot = root;
		spCategory = root.get(0x01);
		spCategorySymbol = root.get(0x02);
		spEvents = root.get(0x03);
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(7001927, 75)
				.append(spRoot)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != FDBSchema.class ) {
			return false;
		}
		FDBSchema o = (FDBSchema) other;
		return new EqualsBuilder()
				.append(o.spRoot, spRoot)
				.build();
	}
	
	public byte[] getTrueBytes() {
		return trueBytes;
	}
	
	public Subspace getSpace() {
		return spRoot;
	}
	
	public Subspace getSpaceCategory() {
		return spCategory;
	}
	
	public byte[] getKeyCategory(String category) {
		return spCategory.pack(category);
	}
	
	public String parseKeyCategory(byte[] key) {
		return spCategory.unpack(key).getString(0);
	}
	
	public Subspace getSpaceCategorySymbol() {
		return spCategorySymbol;
	}
	
	public Subspace getSpaceCategorySymbol(String category) {
		return spCategorySymbol.get(category);
	}
	
	public byte[] getKeyCategorySymbol(String category, String symbol) {
		return spCategorySymbol.pack(Tuple.from(category, symbol));
	}
	
	public byte[] getKeyCategorySymbol(CategorySymbol cs) {
		return getKeyCategorySymbol(cs.getCategory(), cs.getSymbol());
	}
	
	public CategorySymbol parseKeyCategorySymbol(byte[] key) {
		Tuple kt = spCategorySymbol.unpack(key);
		return new CategorySymbol(kt.getString(0), kt.getString(1));
	}
	
	public Subspace getSpaceEvents(String symbol) {
		return spEvents.get(symbol);
	}
	
	public byte[] getKeyEvent(String symbol, long time, int event_id) {
		return spEvents.pack(Tuple.from(symbol, time, event_id));
	}
	
	public byte[] getKeyEvent(EventKey key) {
		return getKeyEvent(key.getSymbol(), key.getTime(), key.getEventID());
	}
	
	public byte[] getKeyEvent(String symbol, long time) {
		return spEvents.pack(Tuple.from(symbol, time));
	}
	
	public EventKey parseKeyEvent(byte[] key) {
		Tuple kt = spEvents.unpack(key);
		return new EventKey(kt.getString(0), kt.getLong(1), (int) kt.getLong(2));
	}
	
	public byte[] packEventData(String event_data) {
		return event_data.getBytes();
	}
	
	public String unpackEventData(byte[] data) {
		return new String(data);
	}
	
	public Pair<EventKey, String> unpackEvent(KeyValue kv) {
		return Pair.of(parseKeyEvent(kv.getKey()), unpackEventData(kv.getValue()));
	}
	
}
