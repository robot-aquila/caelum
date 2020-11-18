package ru.prolib.caelum.service.aggregator;

import java.io.IOException;

import ru.prolib.caelum.service.IBuildingContext;

public class AggregatorServiceBuilder implements IAggregatorServiceBuilder {
	
	protected IAggregatorServiceBuilder createBuilder(String class_name) throws IOException {
		try {
			return (IAggregatorServiceBuilder) Class.forName(class_name).getDeclaredConstructor().newInstance();
		} catch ( Exception e ) {
			throw new IOException("Aggregator service builder instantiation failed", e);
		}
	}
	
	@Override
	public IAggregatorService build(IBuildingContext context) throws IOException {
		return createBuilder(context.getConfig().getAggregatorServiceBuilder()).build(context);
	}
	
	@Override
	public int hashCode() {
		return 20881263;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != AggregatorServiceBuilder.class ) {
			return false;
		}
		return true;
	}

}
