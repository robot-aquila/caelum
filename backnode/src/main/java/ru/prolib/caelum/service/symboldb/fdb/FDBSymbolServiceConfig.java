package ru.prolib.caelum.service.symboldb.fdb;

import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.service.symboldb.SymbolServiceConfig;

public class FDBSymbolServiceConfig extends SymbolServiceConfig {
	public static final String SUBSPACE				= "caelum.symboldb.fdb.subspace";
	public static final String FDB_CLUSTER			= "caelum.symboldb.fdb.cluster";

	public FDBSymbolServiceConfig() {
		super();
	}
	
	@Override
	protected void setDefaults() {
		super.setDefaults();
		props.put(SUBSPACE, "caelum");
		props.put(FDB_CLUSTER, "");
	}
	
	public Subspace getSpace() {
		return new Subspace(Tuple.from(getString(SUBSPACE)));
	}

}
