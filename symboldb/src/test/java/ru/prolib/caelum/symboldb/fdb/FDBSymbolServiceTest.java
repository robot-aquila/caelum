package ru.prolib.caelum.symboldb.fdb;

import static org.junit.Assert.*;

import java.util.HashMap;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.symboldb.ICategoryExtractor;
import ru.prolib.caelum.symboldb.CommonCategoryExtractor;
import ru.prolib.caelum.symboldb.SymbolListRequest;
import ru.prolib.caelum.symboldb.SymbolUpdate;

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
		service = new FDBSymbolService(catExt, schema, 4000);
		service.setDatabase(dbMock);
	}
	
	@Test
	public void testGetters() {
		assertSame(dbMock, service.getDatabase());
		assertSame(catExt, service.getCategoryExtractor());
		assertSame(schema, service.getSchema());
		assertEquals(4000, service.getListSymbolsMaxLimit());
	}
	
	@Test
	public void testRegisterSymbol() {
		expect(dbMock.run(new FDBTransactionRegisterSymbol(schema, catExt, "foobar"))).andReturn(null);
		control.replay();
		
		service.registerSymbol("foobar");
		
		control.verify();
	}
	
	@Test
	public void testRegisterSymbolUpdate() {
		expect(dbMock.run(new FDBTransactionRegisterSymbolUpdate(schema, catExt,
				new SymbolUpdate("kappa", 1872628L, new HashMap<>()))))
			.andReturn(null);
		control.replay();
		
		service.registerSymbolUpdate(new SymbolUpdate("kappa", 1872628L, new HashMap<>()));
		
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
	public void testListSymbolUpdates() {
		expect(dbMock.run(new FDBTransactionListSymbolUpdates(schema, "foo@bar")))
			.andReturn((ICloseableIterator<SymbolUpdate>) itMock);
		control.replay();
		
		assertSame(itMock, service.listSymbolUpdates("foo@bar"));
		
		control.verify();
	}
	
	@Test
	public void testClear() {
		expect(dbMock.run(new FDBTransactionClear(schema))).andReturn(null);
		control.replay();
		
		service.clear();
		
		control.verify();
	}

}
