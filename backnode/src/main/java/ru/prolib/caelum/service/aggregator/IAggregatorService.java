package ru.prolib.caelum.service.aggregator;

import java.util.List;

import ru.prolib.caelum.lib.Interval;
import ru.prolib.caelum.service.AggregatedDataRequest;
import ru.prolib.caelum.service.AggregatedDataResponse;
import ru.prolib.caelum.service.AggregatorStatus;

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
