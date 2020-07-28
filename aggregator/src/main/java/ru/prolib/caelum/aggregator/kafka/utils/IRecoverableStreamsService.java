package ru.prolib.caelum.aggregator.kafka.utils;

import ru.prolib.caelum.aggregator.AggregatorState;
import ru.prolib.caelum.core.IService;

public interface IRecoverableStreamsService extends IService {
	AggregatorState getState();
	void close();
	boolean startAndWaitConfirm(long timeout);
	boolean stopAndWaitConfirm(long timeout);
	boolean waitForStateChangeFrom(AggregatorState from, long timeout);
	boolean waitForStateChangeTo(AggregatorState to, long timeout);
}
