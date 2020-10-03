package ru.prolib.caelum.service;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static ru.prolib.caelum.service.StartupPriority.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.tuple.Pair;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.backnode.RestServiceBuilder;
import ru.prolib.caelum.backnode.rest.jetty.JettyServerBuilder;
import ru.prolib.caelum.lib.HostInfo;
import ru.prolib.caelum.lib.Intervals;
import ru.prolib.caelum.service.aggregator.kafka.KafkaAggregatorServiceBuilder;
import ru.prolib.caelum.service.itemdb.kafka.KafkaItemDatabaseServiceBuilder;
import ru.prolib.caelum.service.itesym.ItesymBuilder;
import ru.prolib.caelum.service.symboldb.CommonCategoryExtractor;
import ru.prolib.caelum.service.symboldb.fdb.FDBSymbolServiceBuilder;

public class GeneralConfigImplTest {
	
	public static void verifyDefaultProperties(Properties props) {
		// common properties
		assertEquals("prod",				props.get("caelum.backnode.mode"));
		assertEquals("localhost",			props.get("caelum.backnode.http.host"));
		assertEquals("9698",				props.get("caelum.backnode.http.port"));
		assertEquals("99",					props.get("caelum.max.errors"));
		assertEquals("60000",				props.get("caelum.default.timeout"));
		assertEquals("15000",				props.get("caelum.shutdown.timeout"));
		assertEquals("5000",				props.get("caelum.max.items.limit"));
		assertEquals("5000",				props.get("caelum.max.symbols.limit"));
		assertEquals("5000",				props.get("caelum.max.events.limit"));
		assertEquals("5000",				props.get("caelum.max.tuples.limit"));
		assertEquals("localhost:8082",		props.get("caelum.kafka.bootstrap.servers"));
		assertEquals("/tmp/kafka-streams",	props.get("caelum.kafka.state.dir"));
		assertEquals("1000",				props.get("caelum.kafka.poll.timeout"));
		assertEquals("caelum-item",			props.get("caelum.kafka.topic.items.name"));
		assertEquals("31536000000000",		props.get("caelum.kafka.topic.items.retention.time"));
		assertEquals("32",					props.get("caelum.kafka.topic.items.num.partitions"));
		assertEquals("1",					props.get("caelum.kafka.topic.items.replication.factor"));
		assertEquals("caelum",				props.get("caelum.fdb.subspace"));
		assertEquals("",					props.get("caelum.fdb.cluster"));
		
		// builders
		assertEquals(KafkaItemDatabaseServiceBuilder.class.getName(),	props.get("caelum.itemdb.builder"));
		assertEquals(FDBSymbolServiceBuilder.class.getName(),			props.get("caelum.symboldb.builder"));
		assertEquals(KafkaAggregatorServiceBuilder.class.getName(),		props.get("caelum.aggregator.builder"));

		// subsystem-related properties
		assertEquals("",									props.get("caelum.itemdb.kafka.transactional.id"));	
		assertEquals(CommonCategoryExtractor.class.getName(),props.get("caelum.symboldb.category.extractor"));
		assertEquals("M1,H1",								props.get("caelum.aggregator.interval"));
		assertEquals("caelum-item-aggregator-",				props.get("caelum.aggregator.kafka.application.id.pfx"));
		assertEquals("caelum-tuple-store-",					props.get("caelum.aggregator.kafka.store.pfx"));
		assertEquals("31536000000000",						props.get("caelum.aggregator.kafka.store.retention.time"));
		assertEquals("caelum-tuple-",						props.get("caelum.aggregator.kafka.target.topic.pfx"));
		assertEquals("",									props.get("caelum.aggregator.kafka.force.parallel.clear"));
		assertEquals("5",									props.get("caelum.aggregator.kafka.linger.ms"));
		assertEquals("2",									props.get("caelum.aggregator.kafka.num.stream.threads"));
		
		// extension-related properties
		String pfx = "caelum.extension.builder.";
		assertEquals("on::" + ItesymBuilder.class.getName(),			props.get(pfx + "Itesym"));
		assertEquals("on::" + RestServiceBuilder.class.getName(),		props.get(pfx + "REST"));
		assertEquals("on:last:" + JettyServerBuilder.class.getName(),	props.get(pfx + "HTTP"));
		assertEquals("caelum-itesym",									props.get("caelum.itesym.kafka.group.id"));
	}

	IMocksControl control;
	Intervals intervals;
	GeneralConfigImpl service;
	
	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		intervals = new Intervals();
		service = new GeneralConfigImpl(intervals);
	}
	
	@Test
	public void testRandomUUID() {
		String actual = service.randomUUID();
		
		assertTrue(actual.matches("^[\\da-z]{8}-[\\da-z]{4}-[\\da-z]{4}-[\\da-z]{4}-[\\da-z]{12}$"));
	}
	
	@Test
	public void testCtor1() {
		assertSame(intervals, service.getIntervals());
	}
	
	@Test
	public void testCtor0( ) {
		service = new GeneralConfigImpl();
		
		assertNotNull(service.getIntervals());
	}

	@Test
	public void testDefaults() {
		assertEquals("app.caelum-backnode.properties", service.getDefaultConfigFile());
		
		verifyDefaultProperties(service.getProperties());
	}
	
	@Test
	public void testDefaults_LoadFromResources1() throws Exception {
		service.getProperties().clear();
		service.loadFromResources(service.getDefaultConfigFile());
		
		verifyDefaultProperties(service.getProperties());
	}
	
	@Test
	public void testDefaults_LoadFromResources0() throws Exception {
		service.getProperties().clear();
		service.loadFromResources();
		
		verifyDefaultProperties(service.getProperties());
	}
	
	@Test
	public void testGetMode() {
		service.getProperties().put("caelum.backnode.mode", "test");
		
		assertEquals(Mode.TEST, service.getMode());
		
		service.getProperties().put("caelum.backnode.mode", "prod");
		
		assertEquals(Mode.PROD, service.getMode());
	}
	
	@Test
	public void testGetMode_ShouldThrowIfUnknownMode() {
		service.getProperties().put("caelum.backnode.mode", "foobar");
		
		assertThrows(IllegalStateException.class, () -> service.getMode());
	}
	
	@Test
	public void testGetHostParameters() {
		service.getProperties().put("caelum.backnode.http.host", "test");
		service.getProperties().put("caelum.backnode.http.port", "9998");
		
		assertEquals("test", service.getHttpHost());
		assertEquals(9998, service.getHttpPort());
		assertEquals(new HostInfo("test", 9998), service.getHttpInfo());
	}
	
	@Test
	public void testGetItemsTopicParameters() {
		service.getProperties().put("caelum.kafka.topic.items.name", "dubby");
		service.getProperties().put("caelum.kafka.topic.items.retention.time", "26899361");
		service.getProperties().put("caelum.kafka.topic.items.num.partitions", "42");
		service.getProperties().put("caelum.kafka.topic.items.replication.factor", "4");
		
		assertEquals("dubby", service.getItemsTopicName());
		assertEquals(26899361L, service.getItemsTopicRetentionTime());
		assertEquals(42, service.getItemsTopicNumPartitions());
		assertEquals(4, service.getItemsTopicReplicationFactor());
	}
	
	@Test
	public void testGetErrorCountersAndCommonTimeouts() {
		service.getProperties().put("caelum.max.errors", "20");
		service.getProperties().put("caelum.default.timeout", "30000");
		service.getProperties().put("caelum.shutdown.timeout", "25000");
		
		assertEquals(20, service.getMaxErrors());
		assertEquals(30000L, service.getDefaultTimeout());
		assertEquals(25000L, service.getShutdownTimeout());
	}
	
	@Test
	public void testGetListLimits() {
		service.getProperties().put("caelum.max.items.limit", "12345");
		service.getProperties().put("caelum.max.symbols.limit", "500");
		service.getProperties().put("caelum.max.events.limit", "7592");
		service.getProperties().put("caelum.max.tuples.limit", "222");
	}
	
	@Test
	public void testGetCommonKafkaProperties() {
		service.getProperties().put("caelum.kafka.bootstrap.servers", "tenance15:2419");
		service.getProperties().put("caelum.kafka.state.dir", "/foo/bar");
		service.getProperties().put("caelum.kafka.poll.timeout", "2500");
		
		assertEquals("tenance15:2419", service.getKafkaBootstrapServers());
		assertEquals("/foo/bar", service.getKafkaStateDir());
		assertEquals(2500L, service.getKafkaPollTimeout());
	}
	
	@Test
	public void testGetCommonFdbProperties() {
		service.getProperties().put("caelum.fdb.subspace", "hello");
		service.getProperties().put("caelum.fdb.cluster", "raw@gap:localhost:4500");
		
		assertEquals("hello", service.getFdbSubspace());
		assertEquals("raw@gap:localhost:4500", service.getFdbCluster());
	}
	
	@Test
	public void testGetExtensions() {
		List<Pair<String, String>> fixture = new ArrayList<>(Arrays.asList(
				Pair.of("caelum.extension.builder.Bamby8", "on:first:bamby.com"),
				Pair.of("caelum.extension.builder.Fluffy", "off:first:pom.fanfa.ron"),
				Pair.of("caelum.extension.builder.Jumbo4", "first:great.site"),
				Pair.of("caelum.extension.builder.Jetta3", "10:gap.gop"),
				Pair.of("caelum.extension.builder.Bobby9", "off:12:pampa.du"),
				Pair.of("caelum.extension.builder.Alpha5", "off::chompy.Clazz"),
				Pair.of("caelum.extension.builder.Gamma1", "::popper.MyClassBuilder"),
				Pair.of("caelum.extension.builder.Gamma2", ":popper.MyClassBuilder"),
				Pair.of("caelum.extension.builder.Gamma3", "popper.MyClassBuilder"),
				Pair.of("caelum.extension.builder.Delta8", "on::foo.bar"),
				Pair.of("caelum.extension.builder.Kappa7", "last:chebby"),
				Pair.of("caelum.extension.builder.Gretta", "off:last:org.vasya")
			));
		Collections.shuffle(fixture);
		Properties props = service.getProperties();
		props.clear();
		fixture.stream().forEach((x) -> props.put(x.getLeft(), x.getRight()));
		
		List<ExtensionConf> expected = Arrays.asList(
				new ExtensionConf("Bamby8", "bamby.com", true, new StartupOrder(FIRST, null)),
				new ExtensionConf("Fluffy", "pom.fanfa.ron", false, new StartupOrder(FIRST, null)),
				new ExtensionConf("Jumbo4", "great.site", true, new StartupOrder(FIRST, null)),
				new ExtensionConf("Jetta3", "gap.gop", true, new StartupOrder(NORMAL, 10)),
				new ExtensionConf("Bobby9", "pampa.du", false, new StartupOrder(NORMAL, 12)),
				new ExtensionConf("Alpha5", "chompy.Clazz", false, new StartupOrder(NORMAL, null)),
				new ExtensionConf("Delta8", "foo.bar", true, new StartupOrder(NORMAL, null)),
				new ExtensionConf("Gamma1", "popper.MyClassBuilder", true, new StartupOrder(NORMAL, null)),
				new ExtensionConf("Gamma2", "popper.MyClassBuilder", true, new StartupOrder(NORMAL, null)),
				new ExtensionConf("Gamma3", "popper.MyClassBuilder", true, new StartupOrder(NORMAL, null)),
				new ExtensionConf("Gretta", "org.vasya", false, new StartupOrder(LAST, null)),
				new ExtensionConf("Kappa7", "chebby", true, new StartupOrder(LAST, null))
			);
		assertEquals(expected, service.getExtensions());
	}
	
	@Test
	public void testGetServiceBuilders() {
		service.getProperties().put("caelum.itemdb.builder", "foo.barClass");
		service.getProperties().put("caelum.symboldb.builder", "gap.myClasse");
		service.getProperties().put("caelum.aggregator.builder", "bobby222.com");
		
		assertEquals("foo.barClass", service.getItemServiceBuilder());
		assertEquals("gap.myClasse", service.getSymbolServiceBuilder());
		assertEquals("bobby222.com", service.getAggregatorServiceBuilder());
	}
	
	@Test
	public void testGetItemServiceKafkaProperties() {
		service.getProperties().put("caelum.itemdb.kafka.transactional.id", "bobby");
		assertEquals("bobby", service.getItemServiceKafkaTransactionalId());
	}
	
	@Test
	public void testGetItemServiceKafkaTransactionalId_ShouldGenerateRandomIdIfNotDefined() {
		service.getProperties().put("caelum.itemdb.kafka.transactional.id", "");
		service = partialMockBuilder(GeneralConfigImpl.class)
				.withConstructor(Intervals.class)
				.withArgs(intervals)
				.addMockedMethod("randomUUID")
				.createMock(control);
		expect(service.randomUUID()).andReturn("046b6c7f-0b8a-43b9-b35d-6489e6daee91");
		control.replay();

		assertEquals("046b6c7f-0b8a-43b9-b35d-6489e6daee91", service.getItemServiceKafkaTransactionalId());
		
		control.verify();
	}
	
	@Test
	public void testGetSymbolServiceCommonProperties() {
		service.getProperties().put("caelum.symboldb.category.extractor", "lumivaara");
		
		assertEquals("lumivaara", service.getSymbolServiceCategoryExtractor());
	}
	
	@Test
	public void testGetAggregatorCommonProperties() {
		service.getProperties().put("caelum.aggregator.interval", "M5,M15,H2");
		
		assertEquals("M5,M15,H2", service.getAggregatorInterval());
	}
	
	@Test
	public void testGetAggregatorServiceKafkaProperties() {
		Properties props = service.getProperties();
		props.put("caelum.backnode.http.host", "furaboll");
		props.put("caelum.backnode.http.port", "5024");
		props.put("caelum.aggregator.kafka.application.id.pfx", "zulu24-");
		props.put("caelum.aggregator.kafka.store.pfx", "my-store-");
		props.put("caelum.aggregator.kafka.store.retention.time", "268916");
		props.put("caelum.aggregator.kafka.target.topic.pfx", "gamma-");
		props.put("caelum.aggregator.kafka.force.parallel.clear", "true");
		props.put("caelum.aggregator.kafka.linger.ms", "10");
		props.put("caelum.aggregator.kafka.num.stream.threads", "8");
		
		assertEquals("zulu24-", service.getAggregatorKafkaApplicationIdPrefix());
		assertEquals("my-store-", service.getAggregatorKafkaStorePrefix());
		assertEquals(268916L, service.getAggregatorKafkaStoreRetentionTime());
		assertEquals("gamma-", service.getAggregatorKafkaTargetTopicPrefix());
		assertTrue(service.getAggregatorKafkaForceParallelClear());
		assertEquals(10L, service.getAggregatorKafkaLingerMs());
		assertEquals(8, service.getAggregatorKafkaNumStreamThreads());
		assertEquals("furaboll:5024", service.getAggregatorKafkaApplicationServer());
	}
	
	@Test
	public void testGetItesymKafkaProperties() {
		service.getProperties().put("caelum.itesym.kafka.group.id", "foo-bar");
		
		assertEquals("foo-bar", service.getItesymKafkaGroupId());
	}
	
	@Test
	public void testSetMode() {
		assertSame(service, service.setMode(Mode.PROD));
		assertEquals(Mode.PROD, service.getMode());
		assertSame(service, service.setMode(Mode.TEST));
		assertEquals(Mode.TEST, service.getMode());
	}
	
	@Test
	public void testSetHttpInfo() {
		assertSame(service, service.setHttpInfo("babbata", 22719));
		assertEquals(new HostInfo("babbata", 22719), service.getHttpInfo());
		assertEquals("babbata", service.getHttpHost());
		assertEquals(22719, service.getHttpPort());
		assertEquals("babbata:22719", service.getAggregatorKafkaApplicationServer());
	}
	
	@Test
	public void testSetItemsTopicProperties() {
		assertSame(service, service.setItemsTopicName("zulu24"));
		assertSame(service, service.setItemsTopicNumPartitions(5));
		assertSame(service, service.setItemsTopicReplicationFactor((short) 2));
		assertSame(service, service.setItemsTopicRetentionTime(689226634L));
		
		assertEquals("zulu24", service.getItemsTopicName());
		assertEquals(5, service.getItemsTopicNumPartitions());
		assertEquals(2, service.getItemsTopicReplicationFactor());
		assertEquals(689226634L, service.getItemsTopicRetentionTime());
	}
	
	@Test
	public void testSetMaxErrors() {
		assertSame(service, service.setMaxErrors(55));
		
		assertEquals(55, service.getMaxErrors());
	}
	
	@Test
	public void testSetCommonTimeouts() {
		assertSame(service, service.setDefaultTimeout(854L));
		assertSame(service, service.setShutdownTimeout(889L));
		
		assertEquals(854L, service.getDefaultTimeout());
		assertEquals(889L, service.getShutdownTimeout());
	}
	
	@Test
	public void testSetKafkaCommonProperties() {
		assertSame(service, service.setKafkaBootstrapServers("127.19.35.14:8801"));
		assertSame(service, service.setKafkaStateDir("/foo/bar"));
		assertSame(service, service.setKafkaPollTimeout(1000L));
		
		assertEquals("127.19.35.14:8801", service.getKafkaBootstrapServers());
		assertEquals("/foo/bar", service.getKafkaStateDir());
		assertEquals(1000L, service.getKafkaPollTimeout());
	}
	
	@Test
	public void testSetFdbCommonProperties() {
		assertSame(service, service.setFdbCluster("foo@bar:localhost:4500"));
		assertSame(service, service.setFdbSubspace("tokama"));
		
		assertEquals("foo@bar:localhost:4500", service.getFdbCluster());
		assertEquals("tokama", service.getFdbSubspace());
	}
	
	@Test
	public void testSetLimitProperties() {
		assertSame(service, service.setMaxItemsLimit(4500));
		assertSame(service, service.setMaxEventsLimit(112));
		assertSame(service, service.setMaxSymbolsLimit(800));
		assertSame(service, service.setMaxTuplesLimit(250));
		
		assertEquals(4500, service.getMaxItemsLimit());
		assertEquals(112, service.getMaxEventsLimit());
		assertEquals(800, service.getMaxSymbolsLimit());
		assertEquals(250, service.getMaxTuplesLimit());
	}
	
	@Test
	public void testSetBuilderProperties() {
		assertSame(service, service.setItemServiceBuilder("tutumbr"));
		assertSame(service, service.setSymbolServiceBuilder("balaur"));
		assertSame(service, service.setAggregatorServiceBuilder("apox"));
		
		assertEquals("tutumbr", service.getItemServiceBuilder());
		assertEquals("balaur", service.getSymbolServiceBuilder());
		assertEquals("apox", service.getAggregatorServiceBuilder());
	}
	
	@Test
	public void testSetSymbolServiceCommonProperties() {
		assertSame(service, service.setSymbolServiceCategoryExtractor("rattakka"));
		
		assertEquals("rattakka", service.getSymbolServiceCategoryExtractor());
	}
	
	@Test
	public void testSetAggregatorCommonProperties() {
		assertSame(service, service.setAggregatorInterval("m24,m15,m6"));
		
		assertEquals("m24,m15,m6", service.getAggregatorInterval());
	}
	
	@Test
	public void testSetAggregatorKafkaProperties() {
		assertSame(service, service.setAggregatorKafkaApplicationIdPrefix("foobar"));
		assertSame(service, service.setAggregatorKafkaStorePrefix("warehouse"));
		assertSame(service, service.setAggregatorKafkaTargetTopicPrefix("stored-data"));
		assertSame(service, service.setAggregatorKafkaForceParallelClear(true));
		assertSame(service, service.setAggregatorKafkaLingerMs(824L));
		assertSame(service, service.setAggregatorKafkaStoreRetentionTime(77756146L));
		assertSame(service, service.setAggregatorKafkaNumStreamThreads(8));
		
		assertEquals("foobar", service.getAggregatorKafkaApplicationIdPrefix());
		assertEquals("warehouse", service.getAggregatorKafkaStorePrefix());
		assertEquals("stored-data", service.getAggregatorKafkaTargetTopicPrefix());
		assertEquals(true, service.getAggregatorKafkaForceParallelClear());
		assertEquals(824L, service.getAggregatorKafkaLingerMs());
		assertEquals(77756146L, service.getAggregatorKafkaStoreRetentionTime());
		assertEquals(8, service.getAggregatorKafkaNumStreamThreads());
	}
	
	@Test
	public void testSetItesymKafkaProperties() {
		assertSame(service, service.setItesymKafkaGroupId("foo-bar-group"));
		
		assertEquals("foo-bar-group", service.getItesymKafkaGroupId());
	}
	
	
}
