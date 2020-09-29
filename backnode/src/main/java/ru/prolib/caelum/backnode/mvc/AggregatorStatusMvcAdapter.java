package ru.prolib.caelum.backnode.mvc;

import ru.prolib.caelum.service.AggregatorState;
import ru.prolib.caelum.service.AggregatorStatus;
import ru.prolib.caelum.service.AggregatorType;

public class AggregatorStatusMvcAdapter {
	private final AggregatorStatus source;
	
	public AggregatorStatusMvcAdapter(AggregatorStatus source) {
		this.source = source;
	}
	
	public String getImplCode() {
		return source.getImplCode();
	}
	
	public String getInterval() {
		return source.getInterval().getCode();
	}
	
	public AggregatorType getType() {
		return source.getType();
	}
	
	public AggregatorState getState() {
		return source.getState();
	}
	
	public Object getStatusInfo() {
		return source.getStatusInfo();
	}

}
