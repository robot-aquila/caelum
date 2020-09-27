package ru.prolib.caelum.lib;

public interface IService {
	void start() throws ServiceException;
	void stop() throws ServiceException;
}
