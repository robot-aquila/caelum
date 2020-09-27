package ru.prolib.caelum.service.aggregator.kafka.utils;

import ru.prolib.caelum.lib.IService;
import ru.prolib.caelum.service.aggregator.AggregatorState;

public interface IRecoverableStreamsService extends IService {
	AggregatorState getState();
	void close();
	boolean startAndWaitConfirm(long timeout);
	boolean stopAndWaitConfirm(long timeout);
	boolean waitForStateChangeFrom(AggregatorState from, long timeout);
	boolean waitForStateChangeTo(AggregatorState to, long timeout);
}
