package ru.prolib.caelum.symboldb.fdb;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.apple.foundationdb.Transaction;

public class FDBTransactionClear extends FDBTransaction<Void> {

	public FDBTransactionClear(FDBSchema schema) {
		super(schema);
	}

	@Override
	public Void apply(Transaction t) {
		t.clear(schema.getSpace().range());
		return null;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(18210157, 349)
				.append(schema)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != FDBTransactionClear.class ) {
			return false;
		}
		FDBTransactionClear o = (FDBTransactionClear) other;
		return new EqualsBuilder()
				.append(o.schema, schema)
				.build();
	}

}
