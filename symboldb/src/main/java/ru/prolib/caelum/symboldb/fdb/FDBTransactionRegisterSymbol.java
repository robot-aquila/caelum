package ru.prolib.caelum.symboldb.fdb;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.apple.foundationdb.Transaction;

import ru.prolib.caelum.symboldb.ICategoryExtractor;

public class FDBTransactionRegisterSymbol extends FDBTransaction<Void> {
	protected final ICategoryExtractor catExt;
	protected final Collection<String> symbols;
	protected final Set<String> categories;
	protected final Map<String, Collection<String>> symbolCategories;

	public FDBTransactionRegisterSymbol(FDBSchema schema, ICategoryExtractor catExt, Collection<String> symbols) {
		super(schema);
		this.catExt = catExt;
		this.symbols = symbols;
		categories = new LinkedHashSet<>();
		symbolCategories = new LinkedHashMap<>();
		for ( String symbol : symbols ) {
			Collection<String> cats = catExt.extract(symbol);
			categories.addAll(cats);
			symbolCategories.put(symbol, cats);
		}
	}

	@Override
	public Void apply(Transaction t) {
		byte[] true_bytes = schema.getTrueBytes();
		for ( String category : categories ) t.set(schema.getKeyCategory(category), true_bytes);
		Iterator<Map.Entry<String, Collection<String>>> it = symbolCategories.entrySet().iterator();
		while ( it.hasNext() ) {
			Map.Entry<String, Collection<String>> entry = it.next();
			for ( String category : entry.getValue() ) {
				t.set(schema.getKeyCategorySymbol(category, entry.getKey()), true_bytes);
			}
		}
		return null;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(115389, 27)
				.append(schema)
				.append(catExt)
				.append(symbols)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != FDBTransactionRegisterSymbol.class ) {
			return false;
		}
		FDBTransactionRegisterSymbol o = (FDBTransactionRegisterSymbol) other;
		return new EqualsBuilder()
				.append(o.schema, schema)
				.append(o.catExt, catExt)
				.append(o.symbols, symbols)
				.build();
	}

}
