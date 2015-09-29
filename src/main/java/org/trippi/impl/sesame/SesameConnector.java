package org.trippi.impl.sesame;

import java.io.IOException;
import java.util.Map;

import org.jrdf.graph.GraphElementFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.stereotype.Component;
import org.trippi.RDFUtil;
import org.trippi.TriplestoreConnector;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;
import org.trippi.TrippiException;
import org.trippi.config.ConfigUtils;
import org.trippi.impl.base.ConcurrentTriplestoreWriter;
import org.trippi.impl.base.ConfigurableSessionPool;
import org.trippi.impl.base.MemUpdateBuffer;
import org.trippi.impl.base.TriplestoreSessionPool;
import org.trippi.impl.base.UpdateBuffer;
import org.trippi.io.TripleIteratorFactory;

/**
 * A <code>TriplestoreConnector</code> for a local, native Sesame RDF
 * triplestore.
 *
 * @author cwilper@cs.cornell.edu
 */
@Component
@Configurable
public class SesameConnector extends TriplestoreConnector {
	private TriplestoreWriter m_writer;

	@Autowired(required = false)
	private TripleIteratorFactory tripleIteratorFactory;

	@Autowired(required = false)
	private Map<String, String> configuration;
	private Logger logger = LoggerFactory.getLogger(SesameConnector.class
			.getName());
	private boolean closed = true;

	public SesameConnector() {
	}

	@Deprecated
	@Override
	public void init(Map<String, String> config) throws TrippiException {
		setConfiguration(config);
	}

	@Override
	public void setConfiguration(Map<String, String> config)
			throws TrippiException {
		configuration = config;
	}

	@Override
	public TriplestoreReader getReader() {
		return getWriter();
	}

	@Override
	public TriplestoreWriter getWriter() {
		if (m_writer == null) {
			try {
				open();
			} catch (TrippiException e) {
				logger.warn(e.getMessage());
			}
		}
		return m_writer;
	}

	protected GraphElementFactory graphElementFactory;

	@Override
	public GraphElementFactory getElementFactory() {
		if (graphElementFactory == null) {
			graphElementFactory = new RDFUtil();
		}
		return graphElementFactory;
	}

	@Override
	public void close() throws TrippiException {
		if (!closed) {
			closed = true;

			try {
				m_writer.flushBuffer();
			} catch (IOException e) {
				e.printStackTrace();
				throw new TrippiException(e.getMessage());
			}
			m_writer.close();
		}
	}

	@Override
	public Map<String, String> getConfiguration() {
		return configuration;
	}

	protected SesameSessionFactory sessionFactory;

	public void setSessionFactory(SesameSessionFactory factory) {
		sessionFactory = factory;
	}

	@Override
	public void open() throws TrippiException {
		if (closed) {
			closed = false;

			Map<String, String> config = getConfiguration();

			TriplestoreSessionPool sessionPool = new ConfigurableSessionPool(
					sessionFactory, ConfigUtils.getRequiredInt(config,
							"initialPoolSize"), ConfigUtils.getRequiredInt(
							config, "maxGrowth"), ConfigUtils.getRequiredInt(
							config, "spareSessions"));
			UpdateBuffer updateBuffer = new MemUpdateBuffer(
					ConfigUtils.getRequiredInt(config, "bufferSafeCapacity"),
					ConfigUtils.getRequiredInt(config, "bufferFlushBatchSize"));
			if (tripleIteratorFactory == null) {
				tripleIteratorFactory = TripleIteratorFactory.defaultInstance();
			}
			try {
				AliasManagedTriplestoreSession updateSession = sessionFactory
						.newSession();
				m_writer = new ConcurrentTriplestoreWriter(sessionPool,
						updateSession.getAliasManager(), updateSession,
						updateBuffer, tripleIteratorFactory,
						ConfigUtils.getRequiredInt(config,
								"autoFlushBufferSize"),
						ConfigUtils.getRequiredInt(config,
								"autoFlushDormantSeconds"));
			} catch (IOException e) {
				throw new TrippiException("Exception when opening connection.",
						e);
			}
		}
	}

	@Override
	public void setTripleIteratorFactory(TripleIteratorFactory arg0) {
		tripleIteratorFactory = arg0;
	}

	@Override
	public void finalize() throws TrippiException {
		close();
	}
}
