package ru.prolib.caelum.service.aggregator.kafka.utils;

/**
 * High-level abstraction streams state listener used in combination with recoverable streams handler.
 */
public interface IRecoverableStreamsHandlerListener {
	
	/**
	 * Called when streams go to running state at first time. Further rebalancing of
	 * the streams will no effect on this callback.
	 * <p>
	 * <b>Warning:</b> Don't call any streams methods during this callback, use executors to schedule calls instead.
	 */
	default void onStarted() { }

	/**
	 * Called when streams go to error state at first time.
	 * It can happen only after streams got to the running state.
	 * <p>
	 * <b>Warning:</b> Don't call any streams methods during this callback, use executors to schedule calls instead.
	 */
	default void onRecoverableError() { }
	
	/**
	 * Called when streams go to error state bypassing running state.
	 * This mean that streams cannot start at first time due to misconfiguration.
	 * <p>
	 * <b>Warning:</b> Don't call any streams methods during this callback, use executors to schedule calls instead.
	 */
	default void onUnrecoverableError() { }
	
	/**
	 * Called when streams was closed. This called just once time and can be called from any other state.
	 * <p>
	 * @param error_on_close - true indicates that there was problems during closing of streams (for example - timeout).
	 * Depending on managing strategy it may consider as signal to stop recovering.
	 */
	default void onClose(boolean error_on_close) { }
	
	/**
	 * Called when streams is temporarily unavailable for example due to rebalancing.
	 * This may called several times after streams got started.
	 * If streams is unavailable some calls of streams may lead to exceptions.
	 */
	default void onUnavailable() { }
	
	/**
	 * Called when streams get back to work to normal running state after unavailability.
	 * This method is always called after previous unavailability signal.
	 * Some cases this may not called for example if streams got to error or shutdown state after rebalancing.
	 */
	default void onAvailable() { }

}
