package ru.prolib.caelum.backnode.rest.jetty.ws;

import javax.servlet.annotation.WebServlet;

import org.eclipse.jetty.websocket.api.WebSocketPolicy;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;

/**
 * Common servlet implementation based on socket creator.
 * See https://www.eclipse.org/jetty/documentation/current/jetty-websocket-server-api.html for details.
 */
@WebServlet
public class WebSocketServletImpl extends WebSocketServlet {
	
	public interface Config {
		Long getIdleTimeout();
		Long getAsyncWriteTimeout();
		Integer getInputBufferSize();
		Integer getMaxBinaryMessageBufferSize();
		Integer getMaxBinaryMessageSize();
		Integer getMaxTextMessageBufferSize();
		Integer getMaxTextMessageSize();
	}
	
	public static class ConfigAdapter implements Config {
		@Override public Long getIdleTimeout() { return null; }
		@Override public Long getAsyncWriteTimeout() { return null; }
		@Override public Integer getInputBufferSize() { return null; }
		@Override public Integer getMaxBinaryMessageBufferSize() { return null; }
		@Override public Integer getMaxBinaryMessageSize() { return null; }
		@Override public Integer getMaxTextMessageBufferSize() { return null; }
		@Override public Integer getMaxTextMessageSize() { return null; }
	}
	
	private static final long serialVersionUID = 1L;
	private final WebSocketCreator socketFactory;
	private final Config config;
	
	public WebSocketServletImpl(WebSocketCreator socketFactory, Config config) {
		this.socketFactory = socketFactory;
		this.config = config;
	}
	
	public WebSocketServletImpl(WebSocketCreator socketFactory) {
		this(socketFactory, null);
	}
	
	public WebSocketCreator getSocketFactory() {
		return socketFactory;
	}
	
	public Config getConfig() {
		return config;
	}

	@Override
	public void configure(WebSocketServletFactory servletFactory) {
		if ( config != null ) {
			Long lv; Integer iv; WebSocketPolicy policy = servletFactory.getPolicy();
			if ( (lv = config.getIdleTimeout()) != null ) policy.setIdleTimeout(lv);
			if ( (lv = config.getAsyncWriteTimeout()) != null ) policy.setAsyncWriteTimeout(lv);
			if ( (iv = config.getInputBufferSize()) != null ) policy.setInputBufferSize(iv);
			if ( (iv = config.getMaxBinaryMessageBufferSize()) != null ) policy.setMaxBinaryMessageBufferSize(iv);
			if ( (iv = config.getMaxBinaryMessageSize()) != null ) policy.setMaxBinaryMessageSize(iv);
			if ( (iv = config.getMaxTextMessageBufferSize()) != null ) policy.setMaxTextMessageBufferSize(iv);
			if ( (iv = config.getMaxTextMessageSize()) != null ) policy.setMaxTextMessageSize(iv);
		}
		servletFactory.setCreator(socketFactory);
	}

}
