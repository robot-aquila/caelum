package ru.prolib.caelum.service.aggregator;

import java.io.IOException;

import ru.prolib.caelum.service.IBuildingContext;

public class AggregatorServiceBuilder implements IAggregatorServiceBuilder {

	protected AggregatorConfig createConfig() {
		return new AggregatorConfig();
	}
	
	protected IAggregatorServiceBuilder createBuilder(String class_name) throws IOException {
		try {
			return (IAggregatorServiceBuilder) Class.forName(class_name).newInstance();
		} catch ( Exception e ) {
			throw new IOException("Aggregator service builder instantiation failed", e);
		}
	}
	
	@Override
	public IAggregatorService build(IBuildingContext context) throws IOException {
		AggregatorConfig config = createConfig();
		config.load(context.getDefaultConfigFileName(), context.getConfigFileName());
		return createBuilder(config.getString(AggregatorConfig.BUILDER)).build(context);
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
