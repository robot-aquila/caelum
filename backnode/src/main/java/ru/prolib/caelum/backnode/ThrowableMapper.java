package ru.prolib.caelum.backnode;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThrowableMapper extends CommonExceptionMapper
	implements ExceptionMapper<Throwable>
{
	private static final Logger logger;
	
	static {
		logger = LoggerFactory.getLogger(ThrowableMapper.class);
	}

	@Override
	public Response toResponse(Throwable e) {
		logger.error("Unhandled exception: ", e);
		return error(500, "Internal server error");
	}

}
