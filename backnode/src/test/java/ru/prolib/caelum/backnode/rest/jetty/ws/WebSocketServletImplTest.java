package ru.prolib.caelum.backnode.rest.jetty.ws;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.eclipse.jetty.websocket.api.WebSocketPolicy;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.junit.Before;
import org.junit.Test;

public class WebSocketServletImplTest {
	
	static class TestConfig implements WebSocketServletImpl.Config {
		Long idleTimeout, asyncWriteTimeout;
		Integer inputBufferSize, maxBinaryMessageBufferSize, maxBinaryMessageSize,
			maxTextMessageBufferSize, maxTextMessageSize;
		@Override public Long getIdleTimeout() { return idleTimeout; }
		@Override public Long getAsyncWriteTimeout() { return asyncWriteTimeout; }
		@Override public Integer getInputBufferSize() { return inputBufferSize; }
		@Override public Integer getMaxBinaryMessageBufferSize() { return maxBinaryMessageBufferSize; }
		@Override public Integer getMaxBinaryMessageSize() { return maxBinaryMessageSize; }
		@Override public Integer getMaxTextMessageBufferSize() { return maxTextMessageBufferSize; }
		@Override public Integer getMaxTextMessageSize() { return maxTextMessageSize; }
	}
	
	IMocksControl control;
	WebSocketCreator socketFactoryMock;
	WebSocketServletFactory servletFactoryMock;
	TestConfig config;
	WebSocketPolicy policy;
	WebSocketServletImpl service, serviceWoConfig;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		socketFactoryMock = control.createMock(WebSocketCreator.class);
		servletFactoryMock = control.createMock(WebSocketServletFactory.class);
		config = new TestConfig();
		policy = new WebSocketPolicy(null);
		service = new WebSocketServletImpl(socketFactoryMock, config);
		serviceWoConfig = new WebSocketServletImpl(socketFactoryMock);
	}
	
	@Test
	public void testCtor2() {
		assertSame(socketFactoryMock, service.getSocketFactory());
		assertSame(config, service.getConfig());
	}
	
	@Test
	public void testCtor1() {
		assertSame(socketFactoryMock, serviceWoConfig.getSocketFactory());
		assertNull(serviceWoConfig.getConfig());
	}
	
	@Test
	public void testConfigure_ShouldNotConfigurePolicyIfNoConfigDefined() {
		servletFactoryMock.setCreator(socketFactoryMock);
		control.replay();
		
		serviceWoConfig.configure(servletFactoryMock);
		
		control.verify();
	}
	
	@Test
	public void testConfigure_ShouldSetIdleTimeoutIfDefined() {
		assertNotEquals((long)(config.idleTimeout = 5000L), policy.getIdleTimeout());
		expect(servletFactoryMock.getPolicy()).andReturn(policy);
		servletFactoryMock.setCreator(socketFactoryMock);
		expectLastCall().andAnswer(() -> { assertEquals(5000L, policy.getIdleTimeout()); return null; });
		control.replay();
		
		service.configure(servletFactoryMock);
		
		control.verify();
	}
	
	@Test
	public void testConfigure_ShouldNotSetIdleTimeoutIfNotDefined() {
		long expected = policy.getIdleTimeout();
		config.idleTimeout = null;
		expect(servletFactoryMock.getPolicy()).andReturn(policy);
		servletFactoryMock.setCreator(socketFactoryMock);
		control.replay();
		
		service.configure(servletFactoryMock);
		
		control.verify();
		assertEquals(expected, policy.getIdleTimeout());
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testConfigure_ShouldSetAsyncWriteTimeoutIfDefined() {
		assertNotEquals((long)(config.asyncWriteTimeout = 2500L), policy.getAsyncWriteTimeout());
		expect(servletFactoryMock.getPolicy()).andReturn(policy);
		servletFactoryMock.setCreator(socketFactoryMock);
		expectLastCall().andAnswer(() -> { assertEquals(2500L, policy.getAsyncWriteTimeout()); return null; });
		control.replay();
		
		service.configure(servletFactoryMock);
		
		control.verify();
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testConfigure_ShouldNotSetAsyncWriteTimeoutIfNotDefined() {
		long expected = policy.getAsyncWriteTimeout();
		config.asyncWriteTimeout = null;
		expect(servletFactoryMock.getPolicy()).andReturn(policy);
		servletFactoryMock.setCreator(socketFactoryMock);
		control.replay();
		
		service.configure(servletFactoryMock);

		control.verify();
		assertEquals(expected, policy.getAsyncWriteTimeout());
	}

	@Test
	public void testConfigure_ShouldSetInputBufferSizeIfDefined() {
		assertNotEquals((int)(config.inputBufferSize = 12345), policy.getInputBufferSize());
		expect(servletFactoryMock.getPolicy()).andReturn(policy);
		servletFactoryMock.setCreator(socketFactoryMock);
		expectLastCall().andAnswer(() -> { assertEquals(12345, policy.getInputBufferSize()); return null; });
		control.replay();
		
		service.configure(servletFactoryMock);
		
		control.verify();
	}
	
	@Test
	public void testConfigure_ShouldNotSetInputBufferSizeIfNotDefined() {
		int expected = policy.getInputBufferSize();
		config.inputBufferSize = null;
		expect(servletFactoryMock.getPolicy()).andReturn(policy);
		servletFactoryMock.setCreator(socketFactoryMock);
		control.replay();
		
		service.configure(servletFactoryMock);
		
		control.verify();
		assertEquals(expected, policy.getInputBufferSize());
	}
	
	@Test
	public void testConfigure_ShouldSetMaxBinaryMessageBufferSizeIfDefined() {
		assertNotEquals((int)(config.maxBinaryMessageBufferSize = 5678), policy.getMaxBinaryMessageBufferSize());
		expect(servletFactoryMock.getPolicy()).andReturn(policy);
		servletFactoryMock.setCreator(socketFactoryMock);
		expectLastCall().andAnswer(() -> { assertEquals(5678, policy.getMaxBinaryMessageBufferSize()); return null; });
		control.replay();
		
		service.configure(servletFactoryMock);
		
		control.verify();
	}
	
	@Test
	public void testConfigure_ShouldNotSetMaxBinaryMessageBufferSizeIfNotDefined() {
		int expected = policy.getMaxBinaryMessageBufferSize();
		config.maxBinaryMessageBufferSize = null;
		expect(servletFactoryMock.getPolicy()).andReturn(policy);
		servletFactoryMock.setCreator(socketFactoryMock);
		control.replay();
		
		service.configure(servletFactoryMock);
		
		control.verify();
		assertEquals(expected, policy.getMaxBinaryMessageBufferSize());
	}

	@Test
	public void testConfigure_ShouldSetMaxBinaryMessageSizeIfDefined() {
		assertNotEquals((int)(config.maxBinaryMessageSize = 5250), policy.getMaxBinaryMessageSize());
		expect(servletFactoryMock.getPolicy()).andReturn(policy);
		servletFactoryMock.setCreator(socketFactoryMock);
		expectLastCall().andAnswer(() -> { assertEquals(5250, policy.getMaxBinaryMessageSize()); return null; });
		control.replay();
		
		service.configure(servletFactoryMock);
		
		control.verify();
	}
	
	@Test
	public void testConfigure_ShouldNotSetMaxBinaryMessageSizeIfNotDefined() {
		int expected = policy.getMaxBinaryMessageSize();
		config.maxBinaryMessageSize = null;
		expect(servletFactoryMock.getPolicy()).andReturn(policy);
		servletFactoryMock.setCreator(socketFactoryMock);
		control.replay();
		
		service.configure(servletFactoryMock);
		
		control.verify();
		assertEquals(expected, policy.getMaxBinaryMessageSize());
	}

	@Test
	public void testConfigure_ShouldSetMaxTextMessageBufferSizeIfDefined() {
		assertNotEquals((int)(config.maxTextMessageBufferSize = 4257), policy.getMaxTextMessageBufferSize());
		expect(servletFactoryMock.getPolicy()).andReturn(policy);
		servletFactoryMock.setCreator(socketFactoryMock);
		expectLastCall().andAnswer(() -> { assertEquals(4257, policy.getMaxTextMessageBufferSize()); return null; });
		control.replay();
		
		service.configure(servletFactoryMock);
		
		control.verify();
	}
	
	@Test
	public void testConfigure_ShouldNotSetMaxTextMessageBufferSizeIfNotDefined() {
		int expected = policy.getMaxTextMessageBufferSize();
		config.maxTextMessageBufferSize = null;
		expect(servletFactoryMock.getPolicy()).andReturn(policy);
		servletFactoryMock.setCreator(socketFactoryMock);
		control.replay();
		
		service.configure(servletFactoryMock);
		
		control.verify();
		assertEquals(expected, policy.getMaxTextMessageBufferSize());
	}

	@Test
	public void testConfigure_ShouldSetMaxTextMessageSizeIfDefined() {
		assertNotEquals((int)(config.maxTextMessageSize = 428), policy.getMaxTextMessageSize());
		expect(servletFactoryMock.getPolicy()).andReturn(policy);
		servletFactoryMock.setCreator(socketFactoryMock);
		expectLastCall().andAnswer(() -> { assertEquals(428, policy.getMaxTextMessageSize()); return null; });
		control.replay();
		
		service.configure(servletFactoryMock);
		
		control.verify();
	}
	
	@Test
	public void testConfigure_ShouldNotSetMaxTextMessageSizeIfNotDefined() {
		int expected = policy.getMaxTextMessageSize();
		config.maxTextMessageSize = null;
		expect(servletFactoryMock.getPolicy()).andReturn(policy);
		servletFactoryMock.setCreator(socketFactoryMock);
		control.replay();
		
		service.configure(servletFactoryMock);
		
		control.verify();
		assertEquals(expected, policy.getMaxTextMessageSize());
	}

}
