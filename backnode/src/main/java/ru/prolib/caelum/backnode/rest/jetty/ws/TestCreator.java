package ru.prolib.caelum.backnode.rest.jetty.ws;

import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;

public class TestCreator implements WebSocketCreator {

	@Override
	public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
		//resp.sendForbidden(message); // TODO: Reject if no more connection slots available
		return new EchoSocket();
	}

}
