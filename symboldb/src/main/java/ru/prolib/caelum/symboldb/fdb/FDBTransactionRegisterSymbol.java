package ru.prolib.caelum.symboldb.fdb;

import java.util.Collection;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.apple.foundationdb.Transaction;

import ru.prolib.caelum.symboldb.ICategoryExtractor;

public class FDBTransactionRegisterSymbol extends FDBTransaction<Void> {
	protected final String symbol;
	protected final ICategoryExtractor catExt;

	public FDBTransactionRegisterSymbol(FDBSchema schema, ICategoryExtractor catExt, String symbol) {
		super(schema);
		this.symbol = symbol;
		this.catExt = catExt;
	}

	@Override
	public Void apply(Transaction t) {
		Collection<String> cats = catExt.extract(symbol);
		for ( String category : cats ) {
			t.set(schema.getKeyCategory(category), schema.getTrueBytes());
			t.set(schema.getKeyCategorySymbol(category, symbol), schema.getTrueBytes());
		}
		return null;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(115389, 27)
				.append(schema)
				.append(catExt)
				.append(symbol)
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
				.append(o.symbol, symbol)
				.build();
	}

}
