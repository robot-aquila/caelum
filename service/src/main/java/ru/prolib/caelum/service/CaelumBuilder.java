package ru.prolib.caelum.service;

import java.io.IOException;

import ru.prolib.caelum.aggregator.AggregatorServiceBuilder;
import ru.prolib.caelum.aggregator.IAggregatorService;
import ru.prolib.caelum.aggregator.IAggregatorServiceBuilder;
import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.itemdb.IItemDatabaseService;
import ru.prolib.caelum.itemdb.IItemDatabaseServiceBuilder;
import ru.prolib.caelum.itemdb.ItemDatabaseServiceBuilder;
import ru.prolib.caelum.symboldb.ISymbolService;
import ru.prolib.caelum.symboldb.ISymbolServiceBuilder;
import ru.prolib.caelum.symboldb.SymbolServiceBuilder;

public class CaelumBuilder {
	private IAggregatorService aggrService;
	private IItemDatabaseService itemDbService;
	private ISymbolService symbolService;
	
	protected IItemDatabaseServiceBuilder createItemDatabaseServiceBuilder() {
		return new ItemDatabaseServiceBuilder();
	}
	
	protected IAggregatorServiceBuilder createAggregatorServiceBuilder() {
		return new AggregatorServiceBuilder();
	}
	
	protected ISymbolServiceBuilder createSymbolServiceBuilder() {
		return new SymbolServiceBuilder();
	}
	
	public CaelumBuilder withAggregatorService(IAggregatorService service) {
		this.aggrService = service;
		return this;
	}
	
	public CaelumBuilder withItemDatabaseService(IItemDatabaseService service) {
		this.itemDbService = service;
		return this;
	}
	
	public CaelumBuilder withSymbolService(ISymbolService service) {
		this.symbolService = service;
		return this;
	}
	
	public ICaelum build() {
		if ( aggrService == null ) {
			throw new NullPointerException("Aggregator service was not defined");
		}
		if ( itemDbService == null ) {
			throw new NullPointerException("ItemDB service was not defined");
		}
		if ( symbolService == null ) {
			throw new NullPointerException("Symbol service was not defined");
		}
		return new Caelum(aggrService, itemDbService, symbolService);
	}
	
	public ICaelum build(String default_config_file, String config_file, CompositeService services)
		throws IOException
	{
		return new Caelum(
				createAggregatorServiceBuilder().build(default_config_file, config_file, services),
				createItemDatabaseServiceBuilder().build(default_config_file, config_file, services),
				createSymbolServiceBuilder().build(default_config_file, config_file, services)
			);
	}
	
}
