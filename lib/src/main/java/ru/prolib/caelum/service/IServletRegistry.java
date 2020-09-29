package ru.prolib.caelum.service;

import javax.servlet.Servlet;

public interface IServletRegistry {
	IServletRegistry registerServlet(ServletMapping servlet);
	IServletRegistry registerServlet(Servlet servlet, String pathSpec);
}