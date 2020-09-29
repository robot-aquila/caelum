package ru.prolib.caelum.service;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;

import org.apache.log4j.BasicConfigurator;
import org.easymock.Capture;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.prolib.caelum.lib.CompositeService;
import ru.prolib.caelum.service.aggregator.AggregatorServiceBuilder;
import ru.prolib.caelum.service.aggregator.IAggregatorServiceBuilder;
import ru.prolib.caelum.service.aggregator.kafka.KafkaAggregatorService;
import ru.prolib.caelum.service.itemdb.IItemDatabaseService;
import ru.prolib.caelum.service.itemdb.IItemDatabaseServiceBuilder;
import ru.prolib.caelum.service.itemdb.ItemDatabaseServiceBuilder;
import ru.prolib.caelum.service.symboldb.ISymbolService;
import ru.prolib.caelum.service.symboldb.ISymbolServiceBuilder;
import ru.prolib.caelum.service.symboldb.SymbolServiceBuilder;

public class CaelumBuilderTest {
	
	static class TestExtBuilder1 implements IExtensionBuilder {
		@Override
		public IExtension build(String x, String y, CompositeService s, ICaelum c) throws IOException {
			throw new UnsupportedOperationException();
		}
	}
	
	@BeforeClass
	public static void setUpBeforeClass() {
		BasicConfigurator.resetConfiguration();
		BasicConfigurator.configure();
	}
	
	IMocksControl control;
	KafkaAggregatorService aggrSvcMock;
	IItemDatabaseService itemDbSvcMock;
	ISymbolService symbolSvcMock;
	CompositeService servicesMock;
	IItemDatabaseServiceBuilder itemDbSvcBuilderMock;
	IAggregatorServiceBuilder aggrSvcBuilderMock;
	ISymbolServiceBuilder symbolSvcBuilderMock;
	IExtensionBuilder extBldrMock1, extBldrMock2, extBldrMock3;
	IExtension extMock1, extMock2, extMock3;
	CaelumBuilder service, mockedService;
	CaelumConfig mockedConfig;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		aggrSvcMock = control.createMock(KafkaAggregatorService.class);
		itemDbSvcMock = control.createMock(IItemDatabaseService.class);
		symbolSvcMock = control.createMock(ISymbolService.class);
		servicesMock = control.createMock(CompositeService.class);
		itemDbSvcBuilderMock = control.createMock(IItemDatabaseServiceBuilder.class);
		aggrSvcBuilderMock = control.createMock(IAggregatorServiceBuilder.class);
		symbolSvcBuilderMock = control.createMock(ISymbolServiceBuilder.class);
		extBldrMock1 = control.createMock(IExtensionBuilder.class);
		extBldrMock2 = control.createMock(IExtensionBuilder.class);
		extBldrMock3 = control.createMock(IExtensionBuilder.class);
		extMock1 = control.createMock(IExtension.class);
		extMock2 = control.createMock(IExtension.class);
		extMock3 = control.createMock(IExtension.class);
		service = new CaelumBuilder();
		mockedService = partialMockBuilder(CaelumBuilder.class)
				.addMockedMethod("createConfig")
				.addMockedMethod("createItemDatabaseServiceBuilder")
				.addMockedMethod("createAggregatorServiceBuilder")
				.addMockedMethod("createSymbolServiceBuilder")
				.addMockedMethod("createExtensionBuilder", String.class)
				.createMock();
		mockedConfig = partialMockBuilder(CaelumConfig.class)
				.withConstructor()
				.addMockedMethod("load", String.class, String.class)
				.createMock();
	}
	
	@Test
	public void testCreateConfig() {
		CaelumConfig actual = service.createConfig();
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateItemDatabaseServiceBuilder() {
		IItemDatabaseServiceBuilder actual = service.createItemDatabaseServiceBuilder();
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(ItemDatabaseServiceBuilder.class)));
	}
	
	@Test
	public void testCreateAggregatorServiceBuilder() {
		IAggregatorServiceBuilder actual = service.createAggregatorServiceBuilder();
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(AggregatorServiceBuilder.class)));
	}
	
	@Test
	public void testCreateSymbolServiceBuilder() {
		ISymbolServiceBuilder actual = service.createSymbolServiceBuilder();
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(SymbolServiceBuilder.class)));
	}
	
	@Test
	public void testCreateExtensionBuilder() throws Exception {
		IExtensionBuilder actual = service.createExtensionBuilder(TestExtBuilder1.class.getName());
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(TestExtBuilder1.class)));
	}
		
	@Test
	public void testBuild3() throws Exception {
		mockedConfig.getProperties().put("caelum.extension.builder.001", "Foo");
		mockedConfig.getProperties().put("caelum.extension.enabled.001", "true");
		mockedConfig.getProperties().put("caelum.extension.builder.002", "Bar");
		mockedConfig.getProperties().put("caelum.extension.enabled.002", "false");
		mockedConfig.getProperties().put("caelum.extension.builder.003", "Gap");
		mockedConfig.getProperties().put("caelum.extension.enabled.003", "true");
		expect(mockedService.createConfig()).andReturn(mockedConfig);
		mockedConfig.load("foo.props", "bar.props");
		expect(mockedService.createAggregatorServiceBuilder()).andReturn(aggrSvcBuilderMock);
		expect(aggrSvcBuilderMock.build("foo.props", "bar.props", servicesMock)).andReturn(aggrSvcMock);
		expect(mockedService.createItemDatabaseServiceBuilder()).andReturn(itemDbSvcBuilderMock);
		expect(itemDbSvcBuilderMock.build("foo.props", "bar.props", servicesMock)).andReturn(itemDbSvcMock);
		expect(mockedService.createSymbolServiceBuilder()).andReturn(symbolSvcBuilderMock);
		expect(symbolSvcBuilderMock.build("foo.props", "bar.props", servicesMock)).andReturn(symbolSvcMock);
		Capture<ICaelum> cap1 = newCapture(), cap2 = newCapture();
		expect(mockedService.createExtensionBuilder("Foo")).andReturn(extBldrMock1);
		expect(extBldrMock1.build(eq("foo.props"),eq("bar.props"),same(servicesMock),capture(cap1))).andReturn(extMock1);
		expect(mockedService.createExtensionBuilder("Gap")).andReturn(extBldrMock3);
		expect(extBldrMock3.build(eq("foo.props"),eq("bar.props"),same(servicesMock),capture(cap2))).andReturn(extMock3);
		control.replay();
		replay(mockedService);
		replay(mockedConfig);
		
		ICaelum actual = mockedService.build("foo.props", "bar.props", servicesMock);
		
		verify(mockedConfig);
		verify(mockedService);
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(Caelum.class)));
		Caelum x = (Caelum) actual;
		assertSame(aggrSvcMock, x.getAggregatorService());
		assertSame(itemDbSvcMock, x.getItemDatabaseService());
		assertSame(symbolSvcMock, x.getSymbolService());
		assertEquals(actual, cap1.getValue());
		assertEquals(actual, cap2.getValue());
		assertEquals(Arrays.asList(extMock1, extMock3), x.getExtensions());
	}

}
