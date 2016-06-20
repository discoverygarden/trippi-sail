package org.trippi.impl.sesame;

import java.lang.ref.WeakReference;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.trippi.TrippiException;
import org.trippi.impl.base.TriplestoreSessionFactory;

public class SesameSessionFactory implements TriplestoreSessionFactory,
ApplicationContextAware {
	private ApplicationContext context;
	protected List<WeakReference<AliasManagedTriplestoreSession>> sessions;
	private Logger logger;

	public SesameSessionFactory() {
		logger = LoggerFactory.getLogger(this.getClass());
		sessions = new LinkedList<WeakReference<AliasManagedTriplestoreSession>>();
	}

	@Override
	public AliasManagedTriplestoreSession newSession() throws TrippiException {
		AliasManagedTriplestoreSession session = context.getBean(AbstractSesameSession.class);
		sessions.add(new WeakReference<AliasManagedTriplestoreSession>(session));
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
		int count = 0;
		for (WeakReference<AliasManagedTriplestoreSession> session_ref : sessions) {
			AliasManagedTriplestoreSession session = session_ref.get();
			synchronized (session) {
				if (session != null && !session.isClosed()) {
					count += 1;
					session.close();
				}
			}
		}
		if (count > 0) {
			// Sessions should be closed by the pool to which they're passed.
			logger.warn("Factory closed %d sessions.", count);
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext arg0)
			throws BeansException {
		context = arg0;
	}

}
