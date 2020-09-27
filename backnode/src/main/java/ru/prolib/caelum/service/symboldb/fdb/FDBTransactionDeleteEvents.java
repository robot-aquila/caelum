package ru.prolib.caelum.service.symboldb.fdb;

import java.util.Collection;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.apple.foundationdb.Transaction;

import ru.prolib.caelum.lib.Events;

public class FDBTransactionDeleteEvents extends FDBTransaction<Void> {
	protected final Collection<Events> events;

	public FDBTransactionDeleteEvents(FDBSchema schema, Collection<Events> events) {
		super(schema);
		this.events = events;
	}

	@Override
	public Void apply(Transaction t) {
		for ( Events e : events ) {
			for ( int event_id : e.getEventIDs() ) {
				t.clear(schema.getKeyEvent(e.getSymbol(), e.getTime(), event_id));
			}
		}
		return null;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(14492617, 75)
				.append(schema)
				.append(events)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != FDBTransactionDeleteEvents.class ) {
			return false;
		}
		FDBTransactionDeleteEvents o = (FDBTransactionDeleteEvents) other;
		return new EqualsBuilder()
				.append(o.schema, schema)
				.append(o.events, events)
				.build();
	}

}
