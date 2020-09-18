package ru.prolib.caelum.symboldb.fdb;

import static org.junit.Assert.*;

import java.util.Arrays;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.lib.Events;
import ru.prolib.caelum.lib.EventsBuilder;
import ru.prolib.caelum.symboldb.ICategoryExtractor;
import ru.prolib.caelum.symboldb.CommonCategoryExtractor;
import ru.prolib.caelum.symboldb.EventListRequest;
import ru.prolib.caelum.symboldb.SymbolListRequest;

@SuppressWarnings("unchecked")
public class FDBSymbolServiceTest {
	IMocksControl control;
	Database dbMock;
	ICloseableIterator<?> itMock;
	ICategoryExtractor catExt;
	FDBSchema schema;
	FDBSymbolService service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		dbMock = control.createMock(Database.class);
		itMock = control.createMock(ICloseableIterator.class);
		catExt = new CommonCategoryExtractor();
		schema = new FDBSchema(new Subspace(Tuple.from("xxx")));
		service = new FDBSymbolService(catExt, schema, 4000, 5000);
		service.setDatabase(dbMock);
	}
	
	@Test
	public void testGetters() {
		assertSame(dbMock, service.getDatabase());
		assertSame(catExt, service.getCategoryExtractor());
		assertSame(schema, service.getSchema());
		assertEquals(4000, service.getListSymbolsMaxLimit());
		assertEquals(5000, service.getListEventsMaxLimit());
	}
	
	@Test
	public void testRegisterSymbol_S() {
		expect(dbMock.run(new FDBTransactionRegisterSymbol(schema, catExt, Arrays.asList("foobar")))).andReturn(null);
		control.replay();
		
		service.registerSymbol("foobar");
		
		control.verify();
	}
	
	@Test
	public void testRegisterSymbol_L() {
		expect(dbMock.run(new FDBTransactionRegisterSymbol(schema, catExt, Arrays.asList("foo@bar", "foo@pop"))))
			.andReturn(null);
		control.replay();
		
		service.registerSymbol(Arrays.asList("foo@bar", "foo@pop"));
		
		control.verify();
	}
	
	@Test
	public void testRegisterEvents_S() {
		Events e = new EventsBuilder()
				.withSymbol("kappa")
				.withTime(1872626L)
				.withEvent(5001, "lumumbr")
				.withEvent(5004, "25.19")
				.build();
		expect(dbMock.run(new FDBTransactionRegisterEvents(schema, catExt, Arrays.asList(e)))).andReturn(null);
		control.replay();
		
		service.registerEvents(e);
		
		control.verify();
	}
	
	@Test
	public void testRegisterEvents_L() {
		Events
			e1 = new EventsBuilder()
				.withSymbol("kappa")
				.withTime(1872626L)
				.withEvent(5001, "lumumbr")
				.withEvent(5004, "25.19")
				.build(),
			e2 = new EventsBuilder()
				.withSymbol("foo@bar")
				.withTime(1728361L)
				.withEvent(3045, "hello, world")
				.build();
		expect(dbMock.run(new FDBTransactionRegisterEvents(schema, catExt, Arrays.asList(e1, e2)))).andReturn(null);
		control.replay();
		
		service.registerEvents(Arrays.asList(e1, e2));
		
		control.verify();
	}
	
	@Test
	public void testDeleteEvents_S() {
		Events e = new EventsBuilder()
				.withSymbol("kappa")
				.withTime(1872626L)
				.withEvent(5001, "lumumbr")
				.withEvent(5004, "25.19")
				.build();
		expect(dbMock.run(new FDBTransactionDeleteEvents(schema, Arrays.asList(e)))).andReturn(null);
		control.replay();
		
		service.deleteEvents(e);
		
		control.verify();
	}
	
	@Test
	public void testDeleteEvents_L() {
		Events
			e1 = new EventsBuilder()
				.withSymbol("kappa")
				.withTime(1872626L)
				.withEvent(5001, "lumumbr")
				.withEvent(5004, "25.19")
				.build(),
			e2 = new EventsBuilder()
				.withSymbol("foo@bar")
				.withTime(1728361L)
				.withEvent(3045, "hello, world")
				.build();
		expect(dbMock.run(new FDBTransactionDeleteEvents(schema, Arrays.asList(e1, e2)))).andReturn(null);
		control.replay();
		
		service.deleteEvents(Arrays.asList(e1, e2));
		
		control.verify();
	}

	@Test
	public void testListCategories() {
		expect(dbMock.run(new FDBTransactionListCategories(schema))).andReturn((ICloseableIterator<String>) itMock);
		control.replay();
		
		assertSame(itMock, service.listCategories());
		
		control.verify();
	}
	
	@Test
	public void testListSymbols() {
		expect(dbMock.run(new FDBTransactionListSymbols(schema, new SymbolListRequest("kappa", null, 200), 4000)))
			.andReturn((ICloseableIterator<String>) itMock);
		control.replay();
		
		assertSame(itMock, service.listSymbols(new SymbolListRequest("kappa", null, 200)));
		
		control.verify();
	}
	
	@Test
	public void testListEvents() {
		EventListRequest request = new EventListRequest("foo@bar");
		expect(dbMock.run(new FDBTransactionListEvents(schema, request, 5000)))
			.andReturn((ICloseableIterator<Events>) itMock);
		control.replay();
		
		assertSame(itMock, service.listEvents(request));
		
		control.verify();
	}
	
	@Test
	public void testClear_ShouldClearIfGlobal() {
		expect(dbMock.run(new FDBTransactionClear(schema))).andReturn(null);
		control.replay();
		
		service.clear(true);
		
		control.verify();
	}
	
	@Test
	public void testClear_ShouldSkipIfLocal() {
		control.replay();
		
		service.clear(false);
		
		control.verify();
	}

}
