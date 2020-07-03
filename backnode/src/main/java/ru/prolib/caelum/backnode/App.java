package ru.prolib.caelum.backnode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.fasterxml.jackson.core.JsonFactory;

import ru.prolib.caelum.aggregator.app.ItemAggregatorBuilder;
import ru.prolib.caelum.backnode.mvc.Freemarker;
import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.core.Periods;
import ru.prolib.caelum.itemdb.kafka.ItemDatabaseService;
import ru.prolib.caelum.service.CaelumBuilder;
import ru.prolib.caelum.service.ICaelum;
import ru.prolib.caelum.symboldb.CommonCategoryExtractor;
import ru.prolib.caelum.symboldb.ISymbolService;
import ru.prolib.caelum.symboldb.fdb.FDBSchema;
import ru.prolib.caelum.symboldb.fdb.FDBServiceStarter;
import ru.prolib.caelum.symboldb.fdb.FDBSymbolService;

public class App {
	static final Logger logger = LoggerFactory.getLogger(App.class);
	
	static ISymbolService createSymbolService(Database db) {
		return new FDBSymbolService(db, new CommonCategoryExtractor(),
				new FDBSchema(new Subspace(Tuple.from("caelum"))));
	}

	public static void main(String[] args) throws Exception {
		FDB fdb = FDB.selectAPIVersion(520);
		Database db = fdb.open();

		AppConfig config = new AppConfig();
		config.load(args.length > 0 ? args[0] : null);
		CompositeService services = new CompositeService();
		ItemAggregatorBuilder item_aggregator_builder = new ItemAggregatorBuilder();
		ICaelum caelum = new CaelumBuilder()
				.withAggregatorService(item_aggregator_builder.getAggregatorService())
				.withItemDatabaseService(new ItemDatabaseService(config.getItemDatabaseConfig()))
				.withSymbolService(createSymbolService(db))
				.build();
		services.register(new FDBServiceStarter(db));
		services.register(item_aggregator_builder.build(config.getItemAggregatorConfig()));
		services.register(new JettyServerBuilder()
			.withHost("127.0.0.1")
			.withPort(60606)
			.withComponent(new NodeService(caelum, new Freemarker(), new JsonFactory(), Periods.getInstance()))
			.build());
		Runtime.getRuntime().addShutdownHook(new Thread(() -> services.stop()));
		services.start();
	}

}
