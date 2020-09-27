package ru.prolib.caelum.service.symboldb.fdb;

import java.util.function.Function;

import com.apple.foundationdb.Transaction;

public abstract class FDBTransaction<R> implements Function<Transaction, R> {
	protected final FDBSchema schema;
	
	public FDBTransaction(FDBSchema schema) {
		this.schema = schema;
	}

}
