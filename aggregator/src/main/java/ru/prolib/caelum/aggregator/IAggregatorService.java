package ru.prolib.caelum.aggregator;

import java.util.List;

import ru.prolib.caelum.core.Interval;

public interface IAggregatorService {
    
    AggregatedDataResponse fetch(AggregatedDataRequest request);
    
    void clear(boolean global);
    
    /**
     * Get aggregation intervals what are provided by this service.
     * <p>
     * @return list of aggregation intervals
     */
    List<Interval> getAggregationIntervals();
    
    List<AggregatorStatus> getAggregatorStatus();

}
