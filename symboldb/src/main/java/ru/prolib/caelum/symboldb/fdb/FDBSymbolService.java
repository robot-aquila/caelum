package ru.prolib.caelum.symboldb.fdb;

import java.util.ArrayList;
import java.util.List;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.core.CloseableIteratorStub;
import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.symboldb.CategorySymbolExtractor;
import ru.prolib.caelum.symboldb.CategorySymbol;
import ru.prolib.caelum.symboldb.SymbolListRequest;
import ru.prolib.caelum.symboldb.SymbolService;
import ru.prolib.caelum.symboldb.SymbolUpdate;

public class FDBSymbolService implements SymbolService {
	private final Database db;
	private final CategorySymbolExtractor catExt;
	private final FDBSchema schema;
	
	public FDBSymbolService(Database db, CategorySymbolExtractor cat_ext, FDBSchema schema) {
		this.db = db;
		this.catExt = cat_ext;
		this.schema = schema;
	}

	@Override
	public void registerSymbol(String symbol) {
		CategorySymbol cs = catExt.extract(symbol);
		byte[] key_cat = schema.getKeyCategory(cs.getCategory());
		byte[] key_sym = schema.getKeyCategorySymbol(cs);
		db.run((t) -> {
			t.set(key_cat, schema.getTrueBytes());
			t.set(key_sym, schema.getTrueBytes());
			return null;
		});
	}

	@Override
	public void registerSymbolUpdate(SymbolUpdate update) {
		CategorySymbol cs  = catExt.extract(update.getSymbol());
		byte[] key_cat = schema.getKeyCategory(cs.getCategory());
		byte[] key_sym = schema.getKeyCategorySymbol(cs);
		KeyValue kv = schema.packSymbolUpdate(update);
		db.run((t) -> {
			t.set(key_cat, schema.getTrueBytes());
			t.set(key_sym, schema.getTrueBytes());
			t.set(kv.getKey(), kv.getValue());
			return null;
		});
	}

	@Override
	public ICloseableIterator<String> listCategories() {
		return new CloseableIteratorStub<>(db.run((t) -> {
			List<String> result = new ArrayList<>();
			AsyncIterator<KeyValue> it = t.getRange(schema.getSpaceCategory().range()).iterator();
			while ( it.hasNext() ) {
				
			}
			
			return result;
		}));
	}
	
	@Override
	public ICloseableIterator<String> listSymbols(SymbolListRequest request) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public ICloseableIterator<SymbolUpdate> listSymbolUpdates(String symbol) {
		// TODO Auto-generated method stub
		return null;
	}

}
