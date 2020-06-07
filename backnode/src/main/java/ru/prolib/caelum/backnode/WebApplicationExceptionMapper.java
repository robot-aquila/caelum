package ru.prolib.caelum.backnode;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class WebApplicationExceptionMapper extends CommonExceptionMapper
	implements ExceptionMapper<WebApplicationException>
{

	@Override
	public Response toResponse(WebApplicationException e) {
		return error(e.getResponse().getStatus(), e.getMessage());
	}

}
