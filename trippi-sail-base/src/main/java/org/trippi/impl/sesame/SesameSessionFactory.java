package org.trippi.impl.sesame;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.trippi.TrippiException;
import org.trippi.impl.base.TriplestoreSessionFactory;

public class SesameSessionFactory implements TriplestoreSessionFactory,
ApplicationContextAware {
	private ApplicationContext context;


	public SesameSessionFactory() {
	}

	@Override
	public AliasManagedTriplestoreSession newSession() throws TrippiException {
		AliasManagedTriplestoreSession session = context.getBean(AbstractSesameSession.class);
		return session;
	}

	@Override
	public String[] listTripleLanguages() {
		return AbstractSesameSession.TRIPLE_LANGUAGES;
	}

	@Override
	public String[] listTupleLanguages() {
		return AbstractSesameSession.TUPLE_LANGUAGES;
	}

	@Override
	public void close() throws TrippiException {
		// No-op.
	}

	@Override
	public void setApplicationContext(ApplicationContext arg0)
			throws BeansException {
		context = arg0;
	}

}
