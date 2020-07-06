package ru.prolib.caelum.symboldb.fdb;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;

import ru.prolib.caelum.core.CloseableIteratorStub;
import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.symboldb.CategorySymbol;
import ru.prolib.caelum.symboldb.SymbolListRequest;

public class FDBTransactionListSymbols extends FDBTransaction<ICloseableIterator<String>> {
	protected final SymbolListRequest request;
	protected final int maxLimit;

	public FDBTransactionListSymbols(FDBSchema schema, SymbolListRequest request, int max_limit) {
		super(schema);
		this.request = request;
		this.maxLimit = max_limit;
	}

	@Override
	public ICloseableIterator<String> apply(Transaction t) {
		Range all_symbols = schema.getSpaceCategorySymbol(request.getCategory()).range();
		byte end[] = all_symbols.end;
		final List<String> result = new ArrayList<>();
		final String after_symbol = request.getAfterSymbol();
		final int limit = Math.min(request.getLimit(), maxLimit);
		if ( after_symbol == null ) {
			byte beg[] = all_symbols.begin;
			AsyncIterator<KeyValue> it = t.getRange(beg, end, limit).iterator();
			while ( it.hasNext() ) {
				result.add(schema.parseKeyCategorySymbol(it.next().getKey()).getSymbol());
			}
		} else {
			byte beg[] = schema.getKeyCategorySymbol(new CategorySymbol(request.getCategory(), after_symbol));
			AsyncIterator<KeyValue> it = t.getRange(beg, end, limit + 1).iterator();
			if ( it.hasNext() ) {
				String first = schema.parseKeyCategorySymbol(it.next().getKey()).getSymbol();
				if ( after_symbol.equals(first) ) {
					while ( it.hasNext() ) {
						result.add(schema.parseKeyCategorySymbol(it.next().getKey()).getSymbol());
					}
				} else {
					int total = 1;
					result.add(first);
					while ( it.hasNext() & total < limit ) {
						result.add(schema.parseKeyCategorySymbol(it.next().getKey()).getSymbol());
						total ++;
					}
				}
			}
		}
		return new CloseableIteratorStub<>(result);
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(1209865, 51)
				.append(schema)
				.append(request)
				.append(maxLimit)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if  ( other == null || other.getClass() != FDBTransactionListSymbols.class ) {
			return false;
		}
		FDBTransactionListSymbols o = (FDBTransactionListSymbols) other;
		return new EqualsBuilder()
				.append(o.schema, schema)
				.append(o.request, request)
				.append(o.maxLimit, maxLimit)
				.build();
	}

}
