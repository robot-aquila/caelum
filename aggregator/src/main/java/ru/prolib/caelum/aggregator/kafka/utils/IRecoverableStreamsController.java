package ru.prolib.caelum.aggregator.kafka.utils;

public interface IRecoverableStreamsController {
	IRecoverableStreamsHandler build(IRecoverableStreamsHandlerListener listener);
	default void onRunning(IRecoverableStreamsHandler handler) { }
	default void onClose(IRecoverableStreamsHandler handler) { }
}
