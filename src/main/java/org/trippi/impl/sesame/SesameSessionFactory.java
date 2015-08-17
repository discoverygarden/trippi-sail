package org.trippi.impl.sesame;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.trippi.TrippiException;
import org.trippi.impl.base.TriplestoreSessionFactory;

public class SesameSessionFactory implements TriplestoreSessionFactory, ApplicationContextAware {
	ApplicationContext context;
	public SesameSessionFactory() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public AliasManagedTriplestoreSession newSession() throws TrippiException {
		return context.getBean(SesameSession.class);
	}

	@Override
	public String[] listTripleLanguages() {
		return SesameSession.TRIPLE_LANGUAGES;
	}

	@Override
	public String[] listTupleLanguages() {
		return SesameSession.TUPLE_LANGUAGES;
	}

	@Override
	public void close() throws TrippiException {
	}

	@Override
	public void setApplicationContext(ApplicationContext arg0)
			throws BeansException {
		context = arg0;
	}

}
