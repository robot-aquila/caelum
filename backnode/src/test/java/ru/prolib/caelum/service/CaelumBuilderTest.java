package ru.prolib.caelum.service;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;

import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;

import org.apache.log4j.BasicConfigurator;
import org.easymock.Capture;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.prolib.caelum.lib.Intervals;
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
		public IExtension build(IBuildingContext context) throws IOException {
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
	BuildingContext contextMock1, contextMock2, contextMock3;
	IItemDatabaseServiceBuilder itemDbSvcBuilderMock;
	IAggregatorServiceBuilder aggrSvcBuilderMock;
	ISymbolServiceBuilder symbolSvcBuilderMock;
	IExtensionBuilder extBldrMock1, extBldrMock2, extBldrMock3;
	IExtension extMock1, extMock2, extMock3;
	CaelumBuilder service, mockedService;
	GeneralConfigImpl mockedConfig;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		aggrSvcMock = control.createMock(KafkaAggregatorService.class);
		itemDbSvcMock = control.createMock(IItemDatabaseService.class);
		symbolSvcMock = control.createMock(ISymbolService.class);
		contextMock1 = control.createMock(BuildingContext.class);
		contextMock2 = control.createMock(BuildingContext.class);
		contextMock3 = control.createMock(BuildingContext.class);
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
		mockedConfig = partialMockBuilder(GeneralConfigImpl.class)
				.withConstructor(Intervals.class)
				.withArgs(new Intervals())
				.addMockedMethod("load", String.class)
				.createMock();
	}
	
	@Test
	public void testCreateConfig() {
		GeneralConfigImpl actual = service.createConfig();
		
		assertNotNull(actual);
		assertNotNull(actual.getIntervals());
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
		expect(contextMock1.getConfigFileName()).andStubReturn("bar.props");
		Properties props = mockedConfig.getProperties();
		props.remove("caelum.extension.builder.Itesym"); // remove builtin extensions
		props.remove("caelum.extension.builder.REST");
		props.remove("caelum.extension.builder.HTTP");
		props.put("caelum.extension.builder.BobExt", "on:1:Bob");
		props.put("caelum.extension.builder.BarExt", "off:2:Bar");
		props.put("caelum.extension.builder.GapExt", "on:3:Gap");
		expect(mockedService.createConfig()).andReturn(mockedConfig);
		mockedConfig.load("bar.props");
		expect(contextMock1.withConfig(mockedConfig)).andReturn(contextMock2);
		expect(mockedService.createAggregatorServiceBuilder()).andReturn(aggrSvcBuilderMock);
		expect(aggrSvcBuilderMock.build(contextMock2)).andReturn(aggrSvcMock);
		expect(mockedService.createItemDatabaseServiceBuilder()).andReturn(itemDbSvcBuilderMock);
		expect(itemDbSvcBuilderMock.build(contextMock2)).andReturn(itemDbSvcMock);
		expect(mockedService.createSymbolServiceBuilder()).andReturn(symbolSvcBuilderMock);
		expect(symbolSvcBuilderMock.build(contextMock2)).andReturn(symbolSvcMock);
		Capture<ICaelum> cap1 = newCapture();
		expect(contextMock2.withCaelum(capture(cap1))).andReturn(contextMock3);
		expect(mockedService.createExtensionBuilder("Bob")).andReturn(extBldrMock1);
		expect(extBldrMock1.build(contextMock3)).andReturn(extMock1);
		// extension BarExt should be skipped because disabled
		expect(mockedService.createExtensionBuilder("Gap")).andReturn(extBldrMock3);
		expect(extBldrMock3.build(contextMock3)).andReturn(extMock3);
		control.replay();
		replay(mockedService);
		replay(mockedConfig);
		
		ICaelum actual = mockedService.build(contextMock1);
		
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
		assertEquals(Arrays.asList(extMock1, extMock3), x.getExtensions());
	}

}
