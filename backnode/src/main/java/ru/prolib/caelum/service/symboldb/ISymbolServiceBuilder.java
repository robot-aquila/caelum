package ru.prolib.caelum.service.symboldb;

import java.io.IOException;

import ru.prolib.caelum.service.IBuildingContext;

public interface ISymbolServiceBuilder {
	/**
	 * General symbol database service builder.
	 * <p>
	 * @param context - building context
	 * @throws IOException - an error occurred
	 */
	ISymbolService build(IBuildingContext context) throws IOException;
}
