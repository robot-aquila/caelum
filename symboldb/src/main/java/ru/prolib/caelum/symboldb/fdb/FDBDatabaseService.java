package ru.prolib.caelum.symboldb.fdb;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;

import ru.prolib.caelum.core.IService;
import ru.prolib.caelum.core.ServiceException;

public class FDBDatabaseService implements IService {
	private static final Logger logger = LoggerFactory.getLogger(FDBDatabaseService.class);
	private final FDBSymbolService target;
	/**
	 * fdb.cluster file content
	 */
	private final String fdbCluster;
	private Database db;
	
	public FDBDatabaseService(FDBSymbolService target, String fdb_cluster) {
		this.target = target;
		this.fdbCluster = fdb_cluster;
	}
	
	public FDBSymbolService getTarget() {
		return target;
	}
	
	public String getFdbCluster() {
		return fdbCluster;
	}
	
	synchronized Database getDatabase() {
		return db;
	}
	
	/**
	 * Service method for testing purposes only.
	 * <p>
	 * @param db - database instance
	 */
	synchronized void setDatabase(Database db) {
		this.db = db;
	}
	
	protected FDB createFDB(int api_version) {
		return FDB.selectAPIVersion(api_version);
	}
	
	protected File createTempFile() throws IOException {
		return File.createTempFile("caelum-", "-fdb.cluster");
	}
	
	protected Writer createWriter(File file) throws IOException {
		return new FileWriter(file);
	}

	@Override
	public synchronized void start() throws ServiceException {
		if ( db != null ) {
			throw new ServiceException("Service already started");
		}
		FDB fdb = createFDB(620);
		if ( fdbCluster == null || fdbCluster.length() == 0 ) {
			db = fdb.open();			
		} else {
			try {
				File temp_file = createTempFile();
				logger.debug("Created temp fdb.cluster file: " + temp_file.getAbsolutePath());
				temp_file.deleteOnExit();
				try ( Writer writer = createWriter(temp_file) ) {
					writer.write(fdbCluster);
				}
				db = fdb.open(temp_file.getAbsolutePath());
			} catch ( IOException e ) {
				throw new ServiceException("Database open failed", e);
			}
		}
		db.options().setTransactionTimeout(15000);
		target.setDatabase(db);
	}

	@Override
	public synchronized void stop() throws ServiceException {
		if ( db != null ) {
			db.close();
			db = null;
		}
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(800917, 703)
				.append(target)
				.append(fdbCluster)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != FDBDatabaseService.class ) {
			return false;
		}
		FDBDatabaseService o = (FDBDatabaseService) other;
		return new EqualsBuilder()
				.append(o.target, target)
				.append(o.fdbCluster, fdbCluster)
				.build();
	}

}
