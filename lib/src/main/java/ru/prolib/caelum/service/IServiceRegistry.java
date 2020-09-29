package ru.prolib.caelum.service;

import ru.prolib.caelum.lib.IService;

public interface IServiceRegistry {
	IServiceRegistry registerService(IService service);
}
