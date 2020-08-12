package ru.prolib.caelum.backnode;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticResourceServlet extends HttpServlet {
	private static final Logger logger = LoggerFactory.getLogger(StaticResourceServlet.class);
	private static final long serialVersionUID = 1L;
	private final File resSrcPath = new File("src/main/resources");
	
	private void sendResource(String path, HttpServletResponse resp) throws IOException {
		OutputStream output = resp.getOutputStream();
		path = StringUtils.stripStart(path, "/");
		File file = new File(resSrcPath, path);
		InputStream input = file.exists() ? new BufferedInputStream(new FileInputStream(file))
				: getClass().getClassLoader().getResourceAsStream(path);
		if ( input == null ) {
			logger.debug("Resource not found: {}", path);
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
		} else {
			try {
				IOUtils.copy(input, output);
			} finally {
				input.close();
			}
		}
	}
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		final String uri = req.getRequestURI();
		if ( uri.endsWith(".html") || uri.endsWith(".htm") ) {
			resp.setContentType("text/html");
			sendResource(uri, resp);
		} else if ( uri.endsWith(".js") ) {
			resp.setContentType("application/javascript");
			sendResource(uri, resp);
		} else if ( uri.endsWith(".css") ) {
			resp.setContentType("text/css");
			sendResource(uri, resp);
		} else {
			resp.sendError(HttpServletResponse.SC_NOT_FOUND);
		}
	}

}
