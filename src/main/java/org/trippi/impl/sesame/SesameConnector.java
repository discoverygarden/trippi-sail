package org.trippi.impl.sesame;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

import org.jrdf.graph.GraphElementFactory;
import org.openrdf.repository.Repository;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.trippi.TriplestoreConnector;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;
import org.trippi.TrippiException;
import org.trippi.config.ConfigUtils;
import org.trippi.impl.base.ConcurrentTriplestoreWriter;
import org.trippi.impl.base.MemUpdateBuffer;
import org.trippi.impl.base.SingleSessionPool;
import org.trippi.impl.base.TriplestoreSessionPool;
import org.trippi.impl.base.UpdateBuffer;
import org.trippi.io.TripleIteratorFactory;

/**
 * A <code>TriplestoreConnector</code> for a local, native Sesame RDF
 * triplestore.
 *
 * @author cwilper@cs.cornell.edu
 */
public class SesameConnector extends TriplestoreConnector {
	private TriplestoreWriter m_writer;
	private GraphElementFactory m_elementFactory;
	private TripleIteratorFactory iteratorFactory;
	private Map<String, String> config;
	private Logger logger = Logger.getLogger(SesameConnector.class.getName());

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
		this.config = config;
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
				logger.warning(e.getMessage());
			}
		}
		return m_writer;
	}

	@Override
	public GraphElementFactory getElementFactory() {
		return m_elementFactory;
	}

	@Override
	public void close() throws TrippiException {
		m_elementFactory = null;
		m_writer.close();
		m_writer = null;
	}

	@Override
	public Map<String, String> getConfiguration() {
		return config;
	}

	@Override
	public void open() throws TrippiException {
		ApplicationContext context = new ClassPathXmlApplicationContext(
				"beans.xml");
		Map<String, String> config = getConfiguration();

		Repository repository = context.getBean("trippiSailRepo",
				Repository.class);

		// TODO: Instantiate session with repository and populate element
		// factory, reader and writer.
		SesameSession session = new SesameSession(repository);
		m_elementFactory = session.getElementFactory();
		TriplestoreSessionPool sessionPool = new SingleSessionPool(session,
				session.listTupleLanguages(), session.listTripleLanguages());
		UpdateBuffer updateBuffer = new MemUpdateBuffer(
				ConfigUtils.getRequiredInt(config, "bufferSafeCapacity"),
				ConfigUtils.getRequiredInt(config, "bufferFlushBatchSize"));
		if (iteratorFactory == null) {
			iteratorFactory = TripleIteratorFactory.defaultInstance();
		}
		try {
			m_writer = new ConcurrentTriplestoreWriter(sessionPool,
					session.getAliasManager(), session, updateBuffer,
					iteratorFactory, ConfigUtils.getRequiredInt(config,
							"autoFlushBufferSize"), ConfigUtils.getRequiredInt(
							config, "autoFlushDormantSeconds"));
		} catch (IOException e) {
			throw new TrippiException("Exception when opening connection.", e);
		}
	}

	@Override
	public void setTripleIteratorFactory(TripleIteratorFactory arg0) {
		iteratorFactory = arg0;
	}
}
