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

public class FDBTransactionListCategories extends FDBTransaction<ICloseableIterator<String>> {

	public FDBTransactionListCategories(FDBSchema schema) {
		super(schema);
	}

	@Override
	public ICloseableIterator<String> apply(Transaction t) {
		List<String> result = new ArrayList<>();
		AsyncIterator<KeyValue> it = t.getRange(schema.getSpaceCategory().range()).iterator();
		while ( it.hasNext() ) {
			result.add(schema.parseKeyCategory(it.next().getKey()));
		}
		return new CloseableIteratorStub<>(result);
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(509727, 21)
				.append(schema)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != FDBTransactionListCategories.class ) {
			return false;
		}
		FDBTransactionListCategories o = (FDBTransactionListCategories) other;
		return new EqualsBuilder()
				.append(o.schema, schema)
				.build();
	}

}
