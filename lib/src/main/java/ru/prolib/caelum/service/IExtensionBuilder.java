package ru.prolib.caelum.service;

import java.io.IOException;

public interface IExtensionBuilder {
	IExtension build(IBuildingContext context) throws IOException;
}
