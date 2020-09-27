package ru.prolib.caelum.service.symboldb.fdb;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.apple.foundationdb.Transaction;

import ru.prolib.caelum.lib.Events;
import ru.prolib.caelum.service.symboldb.ICategoryExtractor;

public class FDBTransactionRegisterEvents extends FDBTransaction<Void> {
	protected final ICategoryExtractor catExt;
	protected final Collection<Events> events;
	protected final Set<String> categories;
	protected final Map<String, Collection<String>> symbolCategories;

	public FDBTransactionRegisterEvents(FDBSchema schema,
			ICategoryExtractor catExt,
			Collection<Events> events,
			Set<String> categoriesCache,
			Map<String, Collection<String>> symbolCategoriesCache)
	{
		super(schema);
		this.catExt = catExt;
		this.events = events;
		this.categories = categoriesCache;
		this.symbolCategories = symbolCategoriesCache;
		for ( Events e : events ) {
			Collection<String> cats = catExt.extract(e.getSymbol());
			this.categories.addAll(cats);
			this.symbolCategories.put(e.getSymbol(), cats);
		}
	}
	
	public FDBTransactionRegisterEvents(FDBSchema schema, ICategoryExtractor catExt, Collection<Events> events) {
		this(schema, catExt, events, new HashSet<>(), new HashMap<>());
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
		for ( Events e : events ) {
			String symbol = e.getSymbol();
			long time = e.getTime();
			for ( int event_id : e.getEventIDs() ) {
				t.set(schema.getKeyEvent(symbol, time, event_id), schema.packEventData(e.getEvent(event_id)));
			}
		}
		return null;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(9000113, 307)
				.append(schema)
				.append(catExt)
				.append(events)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != FDBTransactionRegisterEvents.class ) {
			return false;
		}
		FDBTransactionRegisterEvents o = (FDBTransactionRegisterEvents) other;
		return new EqualsBuilder()
				.append(o.schema, schema)
				.append(o.catExt, catExt)
				.append(o.events, events)
				.build();
	}

}
