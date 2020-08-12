package ru.prolib.caelum.aggregator.kafka.utils;

import org.apache.kafka.streams.KafkaStreams;

public interface IRecoverableStreamsHandler {
	KafkaStreams getStreams();
	boolean closed();
	boolean recoverableError();
	boolean unrecoverableError();
	boolean started();
	void start();
	void close();
	boolean available();
}
