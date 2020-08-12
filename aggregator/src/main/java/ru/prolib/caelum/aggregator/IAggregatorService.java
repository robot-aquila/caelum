package ru.prolib.caelum.aggregator;

import java.util.List;

import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.core.ITuple;
import ru.prolib.caelum.core.Period;

public interface IAggregatorService {
    
    ICloseableIterator<ITuple> fetch(AggregatedDataRequest request);
    
    void clear(boolean global);
    
    /**
     * Get aggregation periods what are provided by this service.
     * <p>
     * @return list of aggregation periods
     */
    List<Period> getAggregationPeriods();
    
    List<AggregatorStatus> getAggregatorStatus();

}
