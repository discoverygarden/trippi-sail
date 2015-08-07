package org.trippi.impl.sesame;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.Node;
import org.jrdf.graph.ObjectNode;
import org.openrdf.query.Binding;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.trippi.RDFUtil;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

public class SesameTupleIterator 
             extends TupleIterator {

    private static final Logger logger = Logger.getLogger(SesameTupleIterator.class.getName());

    private QueryLanguage m_lang;
    private String m_queryText;
    private Repository m_repository;

    private RDFUtil m_util;
    
    private TupleQueryResult result;

    public SesameTupleIterator(QueryLanguage lang,
                               String queryText,
                               Repository repository) throws TrippiException {
        m_lang       = lang;
        m_queryText  = queryText;
        m_repository = repository;

        try { m_util = new RDFUtil(); } catch (Exception e) { } // won't happen

		try {
			TupleQuery query = m_repository.getConnection().prepareTupleQuery(m_lang, m_queryText);
			result = query.evaluate();
		} catch (Exception e) {
			throw new TrippiException("Exception while running query.", e);
		}
    }

    //////////////////////////////////////////////////////////////////////////
    //////////////// TupleIterator ///////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////

    /**
     * Get the names of the binding variables.
     *
     * These will be the keys in the map for result.
     */
    public String[] names() throws TrippiException {
        try {
			return (String[]) result.getBindingNames().toArray();
		} catch (QueryEvaluationException e) {
			throw new TrippiException("Exception in names().", e);
		}
    }

    /**
     * Return true if there are any more tuples.
     */
    public boolean hasNext() throws TrippiException {
        try {
			return result.hasNext();
		} catch (Exception e) {
			throw new TrippiException("Exception in hasNext().", e);
		}
    }
    
    /**
     * Return the next tuple.
     */
    public Map<String, Node> next() throws TrippiException {
    	Map<String, Node> to_return = new HashMap<String, Node>();
    	try {
			for (Binding i: result.next()) {
				to_return.put(i.getName(), objectNode(i.getValue()));
			}
		} catch (Exception e) {
			throw new TrippiException("Exception in next().", e);
		}
        return to_return;
    }

    /**
     * Release resources held by this iterator.
     */
    public void close() throws TrippiException {
        try {
			result.close();
		} catch (Exception e) {
			throw new TrippiException("Exception in close().", e);
		}
    }

    private ObjectNode objectNode(org.openrdf.model.Value object)
            throws GraphElementFactoryException,
                   URISyntaxException {
        if (object == null) return null;
        if (object instanceof org.openrdf.model.URI) {
            return m_util.createResource( new URI(((org.openrdf.model.URI) object).toString()) );
        } else if (object instanceof  org.openrdf.model.Literal) {
            org.openrdf.model.Literal lit = (org.openrdf.model.Literal) object;
            org.openrdf.model.URI uri = lit.getDatatype();
            String lang = lit.getLanguage();
            if (uri != null) {
                // typed 
                return m_util.createLiteral(lit.getLabel(), new URI(uri.toString()));
            } else if (lang != null && !lang.equals("")) {
                // local
                return m_util.createLiteral(lit.getLabel(), lang);
            } else {
                // plain
                return m_util.createLiteral(lit.getLabel());
            }
        } else {
            return m_util.createResource(((org.openrdf.model.BNode) object).getID().hashCode());
        }
    }

}