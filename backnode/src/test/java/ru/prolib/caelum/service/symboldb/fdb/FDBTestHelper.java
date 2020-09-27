package ru.prolib.caelum.service.symboldb.fdb;

import java.io.Closeable;
import java.io.IOException;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

public class FDBTestHelper implements Closeable {
	public static final String DEFAULT_SPACE = "caelum_test";
	
	private FDB fdb;
	private Database db;
	private Subspace rootSubspace;
	
	public FDBTestHelper(Object subspace, String fdb_cluster_file) {
		fdb = FDB.selectAPIVersion(620);
		db = fdb_cluster_file == null ? fdb.open() : fdb.open(fdb_cluster_file);
		rootSubspace = new Subspace(Tuple.from(subspace));
	}
	
	public FDBTestHelper(String fdb_cluster_file) {
		this(DEFAULT_SPACE, fdb_cluster_file);
	}
	
	public FDBTestHelper() {
		this(null);
	}
	
	public Database getDB() {
		return db;
	}
	
	public Subspace getTestSubspace() {
		return rootSubspace;
	}
	
	public Subspace childSubspace(Object subspace) {
		return rootSubspace.subspace(Tuple.from(subspace));
	}
	
	public void clearTestSubspace() {
		db.run((Transaction tr) -> {
			tr.clear(rootSubspace.range());
			return null;
		});
	}

	@Override
	public void close() throws IOException {
		clearTestSubspace();
		db.close();
	}

}
