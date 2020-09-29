package ru.prolib.caelum.service.symboldb.fdb;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.tuple.Pair;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;

import ru.prolib.caelum.lib.Events;
import ru.prolib.caelum.lib.EventsBuilder;
import ru.prolib.caelum.lib.ICloseableIterator;
import ru.prolib.caelum.lib.IteratorStub;
import ru.prolib.caelum.service.EventListRequest;
import ru.prolib.caelum.service.symboldb.EventKey;

public class FDBTransactionListEvents extends FDBTransaction<ICloseableIterator<Events>> {
	protected final EventListRequest request;
	protected final int maxLimit;

	public FDBTransactionListEvents(FDBSchema schema, EventListRequest request, int maxLimit) {
		super(schema);
		this.request = request;
		this.maxLimit = maxLimit;
	}
	
	@Override
	public ICloseableIterator<Events> apply(Transaction t) {
		final String symbol = request.getSymbol();
		Range all_events = schema.getSpaceEvents(symbol).range();
		byte beg[] = all_events.begin, end[] = all_events.end;
		List<Events> result = new ArrayList<>();
		int limit = maxLimit;
		if ( request.getLimit() != null ) {
			limit = Math.min(request.getLimit(), maxLimit);
		}
		if ( request.getFrom() != null ) {
			beg = schema.getKeyEvent(symbol, request.getFrom());
		}
		if ( request.getTo() != null ) {
			end = schema.getKeyEvent(symbol, request.getTo());
		}
		AsyncIterator<KeyValue> it = t.getRange(beg, end).iterator();
		String cur_symbol = null;
		Long cur_time = null;
		EventsBuilder builder = new EventsBuilder();
		while ( it.hasNext() ) {
			Pair<EventKey, String> row = schema.unpackEvent(it.next());
			EventKey key = row.getKey();
			if ( cur_symbol == null ) {
				builder.withSymbol(cur_symbol = key.getSymbol())
					.withTime(cur_time = key.getTime())
					.withEvent(key.getEventID(), row.getRight());
			} else if ( cur_symbol.equals(key.getSymbol()) && cur_time.equals(key.getTime()) ) {
				builder.withEvent(key.getEventID(), row.getRight());
			} else {
				result.add(builder.build());
				builder = new EventsBuilder();
				if ( result.size() >= limit ) {
					break;
				}
				builder.withSymbol(cur_symbol = key.getSymbol())
					.withTime(cur_time = key.getTime())
					.withEvent(key.getEventID(), row.getRight());
			}
		}
		if ( builder.hasEvents() ) {
			result.add(builder.build());
		}
		return new IteratorStub<>(result);
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(1699771, 89)
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
		if ( other == null || other.getClass() != FDBTransactionListEvents.class ) {
			return false;
		}
		FDBTransactionListEvents o = (FDBTransactionListEvents) other;
		return new EqualsBuilder()
				.append(o.schema, schema)
				.append(o.request, request)
				.append(o.maxLimit, maxLimit)
				.build();
	}

}
