package ru.prolib.caelum.service;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import ru.prolib.caelum.aggregator.AggregatorServiceBuilder;
import ru.prolib.caelum.aggregator.IAggregatorServiceBuilder;
import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.core.ExecutorService;
import ru.prolib.caelum.itemdb.IItemDatabaseServiceBuilder;
import ru.prolib.caelum.itemdb.ItemDatabaseServiceBuilder;
import ru.prolib.caelum.symboldb.ISymbolService;
import ru.prolib.caelum.symboldb.ISymbolServiceBuilder;
import ru.prolib.caelum.symboldb.SymbolServiceBuilder;

public class CaelumBuilder {
	
	protected IItemDatabaseServiceBuilder createItemDatabaseServiceBuilder() {
		return new ItemDatabaseServiceBuilder();
	}
	
	protected IAggregatorServiceBuilder createAggregatorServiceBuilder() {
		return new AggregatorServiceBuilder();
	}
	
	protected ISymbolServiceBuilder createSymbolServiceBuilder() {
		return new SymbolServiceBuilder();
	}
	
	protected java.util.concurrent.ExecutorService createExecutor() {
		return Executors.newCachedThreadPool();
	}
	
	protected ISymbolCache createSymbolCache(ISymbolService symbolService, CompositeService services) {
		java.util.concurrent.ExecutorService executor = createExecutor();
		services.register(new ExecutorService(executor, 15000L));
		return new SymbolCache(symbolService, executor, new ConcurrentHashMap<>());
	}
	
	public ICaelum build(String default_config_file, String config_file, CompositeService services)
		throws IOException
	{
		ISymbolService symbolService = createSymbolServiceBuilder().build(default_config_file, config_file, services); 
		return new Caelum(
				createAggregatorServiceBuilder().build(default_config_file, config_file, services),
				createItemDatabaseServiceBuilder().build(default_config_file, config_file, services),
				symbolService,
				createSymbolCache(symbolService, services)
			);
	}
	
}
