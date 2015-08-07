package org.trippi.impl.sesame;

import java.util.Map;
import org.apache.log4j.Logger;
import org.jrdf.graph.GraphElementFactory;
import org.openrdf.repository.Repository;
import org.trippi.TriplestoreConnector;
import org.trippi.TriplestoreReader;
import org.trippi.TriplestoreWriter;
import org.trippi.TrippiException;
import org.trippi.impl.base.ConcurrentTriplestoreWriter;
import org.trippi.io.TripleIteratorFactory;

/**
 * A <code>TriplestoreConnector</code> for a local, native Sesame RDF
 * triplestore.
 *
 * @author cwilper@cs.cornell.edu
 */
public class SesameConnector extends TriplestoreConnector {
    private static final Logger logger = Logger.getLogger(SesameConnector.class.getName());

    private ConcurrentTriplestoreWriter m_writer;
    private GraphElementFactory m_elementFactory;

    public SesameConnector() {
    }

    @Deprecated
    @Override
    public void init(Map<String, String> config) throws TrippiException {
    	setConfiguration(config);
    }
    
    @Override
    public void setConfiguration(Map<String, String> config) throws TrippiException {
        //AliasManager aliasManager = new DefaultAliasManager(new HashMap<String, String>());

        // TODO: Initialize Sail Repository
        Repository repository = null;

        // TODO: Instantiate session with repository and populate element factory, reader and writer.
    }

    @Override
    public TriplestoreReader getReader() {
        // TODO: Get reader.
    }

    @Override
    public TriplestoreWriter getWriter() {
        return m_writer;
    }

    @Override
    public GraphElementFactory getElementFactory() {
        return m_elementFactory;
    }

    @Override
    public void close() throws TrippiException {
        m_writer.close();
    }

	@Override
	public Map<String, String> getConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void open() throws TrippiException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setTripleIteratorFactory(TripleIteratorFactory arg0) {
		// TODO Auto-generated method stub
		
	}

}
