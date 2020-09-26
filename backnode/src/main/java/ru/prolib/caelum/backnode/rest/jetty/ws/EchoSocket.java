package ru.prolib.caelum.backnode.rest.jetty.ws;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WriteCallback;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@WebSocket
public class EchoSocket {
	private static final Logger logger = LoggerFactory.getLogger(EchoSocket.class);

	@OnWebSocketConnect
	public void onConnect(Session session) {
		logger.debug("Connected: from=" + session.getRemoteAddress().getAddress());
		try {
			session.getRemote().sendString("Hello from server!");
		} catch ( IOException e ) {
			logger.error("IO error: ", e);
		}
	}
	
	@OnWebSocketClose
	public void onClose(Session session, int statusCode, String reason) {
		logger.debug("Closed connection: from=" + session.getRemoteAddress().getAddress()
				+ " status=" + statusCode
				+ " reason=" + reason);
	}
	
	@OnWebSocketMessage
	public void onMessage(Session session, String msg) {
		logger.debug("Message received: from=" + session.getRemoteAddress().getAddress() + " message=" + msg);
		session.getRemote().sendString("You have sent: " + msg, new WriteCallback() {
			
			@Override
			public void writeFailed(Throwable x) {
				logger.debug("writeFailed: ", x);
			}
			
			@Override
			public void writeSuccess() {
				logger.debug("writeSuccess");
				CompletableFuture.runAsync(() -> {
					try {
						Thread.sleep(5000);
						if ( session.isOpen() ) {
							ByteBuffer payload = ByteBuffer.wrap("hb".getBytes());
							session.getRemote().sendPing(payload);
							logger.debug("Ping/Pong");
						}
					} catch ( Exception e ) { }
				});
			}
			
		});
	}
	
	@OnWebSocketError
	public void onError(Session session, Throwable e) {
		logger.error("Error: from=" + session.getRemoteAddress().getAddress(), e);
	}
	
}
