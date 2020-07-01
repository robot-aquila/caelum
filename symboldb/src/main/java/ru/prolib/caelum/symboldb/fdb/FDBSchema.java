package ru.prolib.caelum.symboldb.fdb;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.symboldb.CategorySymbol;
import ru.prolib.caelum.symboldb.SymbolTime;
import ru.prolib.caelum.symboldb.SymbolUpdate;

public class FDBSchema {
	private static final byte[] trueBytes =  Tuple.from(true).pack();
			
	private final Subspace
		spRoot,
		spCategory,
		spCategorySymbol,
		spSymbolUpdate;
	
	public FDBSchema(Subspace root) {
		spRoot = root;
		spCategory = root.get(0x01);
		spCategorySymbol = root.get(0x02);
		spSymbolUpdate = root.get(0x03);
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
	
	public byte[] packTokens(Map<Integer, String> tokens) {
		Tuple tuple = new Tuple();
		Iterator<Map.Entry<Integer, String>> it = tokens.entrySet().iterator();
		while ( it.hasNext() ) {
			Map.Entry<Integer, String> entry = it.next();
			tuple = tuple.add(entry.getKey()).add(entry.getValue());
		}
		return tuple.pack();
	}
	
	public Map<Integer, String> unpackTokens(byte[] packed) {
		Tuple tuple = Tuple.fromBytes(packed);
		int count = tuple.size();
		if ( count % 2 != 0 ) {
			throw new IllegalArgumentException("Uneven amount of elements");
		}
		Map<Integer, String> result = new LinkedHashMap<>();
		for ( int i = 0; i < count; i += 2 ) {
			result.put((int) tuple.getLong(i), tuple.getString(i + 1));
		}
		return result;
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
	
	public Subspace getSpaceSymbolUpdate(String symbol) {
		return spSymbolUpdate.get(symbol);
	}
	
	public byte[] getKeySymbolUpdate(String symbol, long time) {
		return spSymbolUpdate.pack(Tuple.from(symbol, time));
	}
	
	public byte[] getKeySymbolUpdate(SymbolTime st) {
		return getKeySymbolUpdate(st.getSymbol(), st.getTime());
	}
	
	public SymbolTime parseKeySymbolUpdate(byte[] key) {
		Tuple kt = spSymbolUpdate.unpack(key);
		return new SymbolTime(kt.getString(0), kt.getLong(1));
	}
	
	public KeyValue packSymbolUpdate(SymbolUpdate update) {
		return new KeyValue(getKeySymbolUpdate(update.getSymbol(), update.getTime()), packTokens(update.getTokens()));
	}
	
	public SymbolUpdate unpackSymbolUpdate(KeyValue kv) {
		SymbolTime pk = parseKeySymbolUpdate(kv.getKey());
		return new SymbolUpdate(pk.getSymbol(), pk.getTime(), unpackTokens(kv.getValue()));
	}
	
}
