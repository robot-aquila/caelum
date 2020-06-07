package ru.prolib.caelum.backnode;

public class ResultCode {
	public static final int OK = 0;

	public static final int ERR_ENTITY_ERROR = 4403;
	public static final int ERR_ENTITY_NOT_FOUND = 4404;
	public static final int ERR_ENTITY_EXISTS = 4405;
	public static final int ERR_INVALID_VALUE = 4406;
	
	public static final int ERR_INTERRUPTED = 5010;
	public static final int ERR_EXECUTION_ERROR = 5011;
	public static final int ERR_TIMEOUT = 5012;
	public static final int ERR_TRANSACTION_BUILDER_STATE = 5013;
	public static final int ERR_TRANSACTION_BUILDER_ARG = 5014;
	public static final int ERR_DOCUMENT_BUILDER_STATE = 5015;
	public static final int ERR_DOCUMENT_BUILDER_ARG = 5016;
	public static final int ERR_DB_TRANSACTION_FAILED = 5017;
	public static final int ERR_CORRUPTED_DATA = 5018;
	public static final int ERR_NO_MORE_ATTEMPTS = 5019;
	public static final int ERR_SERVICE_TERMINATED = 5020;
	public static final int ERR_EXECUTION_CANCELLED = 5021;
	public static final int ERR_SERVICE_UNAVAILABLE = 5022;
	
}
