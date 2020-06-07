package ru.prolib.caelum.backnode;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class CommonExceptionMapper {

	protected long getTime() {
		return System.currentTimeMillis();
	}
	
	protected Response error(int http_code, int error_code, String error_message, long time) {
		return Response.status(http_code)
				.type(MediaType.APPLICATION_JSON)
				.entity(new Result<Void>(time, error_code, error_message, null))
				.build();
	}
	
	protected Response error(int http_code, int error_code, String error_message) {
		return error(http_code, error_code, error_message, getTime());
	}
	
	protected Response error(int error_code, String error_message, long time) {
		return error(error_code, error_code, error_message, time);
	}
	
	protected Response error(int error_code, String error_message) {
		return error(error_code, error_message, getTime());
	}

}
