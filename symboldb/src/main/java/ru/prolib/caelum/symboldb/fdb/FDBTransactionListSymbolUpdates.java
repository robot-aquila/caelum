package ru.prolib.caelum.symboldb.fdb;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;

import ru.prolib.caelum.core.CloseableIteratorStub;
import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.symboldb.SymbolUpdate;

public class FDBTransactionListSymbolUpdates extends FDBTransaction<ICloseableIterator<SymbolUpdate>> {
	protected final String symbol;

	public FDBTransactionListSymbolUpdates(FDBSchema schema, String symbol) {
		super(schema);
		this.symbol = symbol;
	}

	@Override
	public ICloseableIterator<SymbolUpdate> apply(Transaction t) {
		List<SymbolUpdate> result = new ArrayList<>();
		AsyncIterator<KeyValue> it = t.getRange(schema.getSpaceSymbolUpdate(symbol).range()).iterator();
		while ( it.hasNext() ) {
			result.add(schema.unpackSymbolUpdate(it.next()));
		}
		return new CloseableIteratorStub<>(result);
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(1699771, 89)
				.append(schema)
				.append(symbol)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != FDBTransactionListSymbolUpdates.class ) {
			return false;
		}
		FDBTransactionListSymbolUpdates o = (FDBTransactionListSymbolUpdates) other;
		return new EqualsBuilder()
				.append(o.schema, schema)
				.append(o.symbol, symbol)
				.build();
	}

}
