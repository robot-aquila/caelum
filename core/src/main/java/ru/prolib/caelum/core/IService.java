package ru.prolib.caelum.core;

public interface IService {
	void start() throws ServiceException;
	void stop() throws ServiceException;
}
