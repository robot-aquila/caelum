package ru.prolib.caelum.lib;

public class ServiceException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	
	public ServiceException() {
		
	}
	
	public ServiceException(String msg) {
		super(msg);
	}
	
	public ServiceException(String msg, Throwable t) {
		super(msg, t);
	}
	
	public ServiceException(Throwable t) {
		super(t);
	}

}
