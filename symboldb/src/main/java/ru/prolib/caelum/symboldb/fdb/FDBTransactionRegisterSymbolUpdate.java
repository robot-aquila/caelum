package ru.prolib.caelum.symboldb.fdb;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;

import ru.prolib.caelum.symboldb.CategoryExtractor;
import ru.prolib.caelum.symboldb.SymbolUpdate;

public class FDBTransactionRegisterSymbolUpdate extends FDBTransactionRegisterSymbol {
	protected final SymbolUpdate update;

	public FDBTransactionRegisterSymbolUpdate(FDBSchema schema, CategoryExtractor catExt, SymbolUpdate update) {
		super(schema, catExt, update.getSymbol());
		this.update = update;
	}
	
	@Override
	public Void apply(Transaction t) {
		super.apply(t);
		KeyValue kv = schema.packSymbolUpdate(update);
		t.set(kv.getKey(), kv.getValue());
		return null;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(9000113, 307)
				.append(schema)
				.append(catExt)
				.append(update)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != FDBTransactionRegisterSymbolUpdate.class ) {
			return false;
		}
		FDBTransactionRegisterSymbolUpdate o = (FDBTransactionRegisterSymbolUpdate) other;
		return new EqualsBuilder()
				.append(o.schema, schema)
				.append(o.catExt, catExt)
				.append(o.update, update)
				.build();
	}

}
