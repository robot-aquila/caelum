package ru.prolib.caelum.backnode.mvc;

import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;

public class Freemarker extends Configuration {
	
	public Freemarker() {
		super(Configuration.VERSION_2_3_29);
		setClassForTemplateLoading(getClass(), "/templates/freemaker");
		setDefaultEncoding("UTF-8");
		setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
		setLogTemplateExceptions(false);
		setWrapUncheckedExceptions(true);
		setFallbackOnNullLoopVariable(false);
	}

}
