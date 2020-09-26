package ru.prolib.caelum.backnode.rest.jetty.ws;

import javax.servlet.annotation.WebServlet;

import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

@WebServlet
public class EchoSocketServlet extends WebSocketServlet {
	private static final long serialVersionUID = 1L;

	@Override
	public void configure(WebSocketServletFactory factory) {
		factory.getPolicy().setIdleTimeout(10000);
		factory.register(EchoSocket.class);
	}

}
