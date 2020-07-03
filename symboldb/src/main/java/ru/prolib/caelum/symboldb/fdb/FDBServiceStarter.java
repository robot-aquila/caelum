package ru.prolib.caelum.symboldb.fdb;

import com.apple.foundationdb.Database;

import ru.prolib.caelum.core.IService;
import ru.prolib.caelum.core.ServiceException;

public class FDBServiceStarter implements IService {
	private final Database db;
	
	public FDBServiceStarter(Database db) {
		this.db = db;
	}

	@Override
	public void start() throws ServiceException {
		
	}

	@Override
	public void stop() throws ServiceException {
		db.close();
	}

}
