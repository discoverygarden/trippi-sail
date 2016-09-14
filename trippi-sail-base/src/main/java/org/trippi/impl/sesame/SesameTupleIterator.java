package org.trippi.impl.sesame;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.Node;
import org.jrdf.graph.ObjectNode;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.trippi.RDFUtil;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;

public class SesameTupleIterator extends TupleIterator {

    private RDFUtil m_util;

    private TupleQueryResult result;

    public SesameTupleIterator(QueryLanguage lang, String queryText, RepositoryConnection connection, Dataset dataset)
            throws TrippiException {

        try {
            m_util = new RDFUtil();
        }
        catch (Exception e) {
        } // won't happen

        if (queryText.toLowerCase().trim().startsWith("ask")) {
            try {
                BooleanQuery query = connection.prepareBooleanQuery(lang, queryText);
                query.setDataset(dataset);
                result = new BooleanTupleResult(query.evaluate());
            }
            catch (Exception e) {
                throw new TrippiException("Exception while running ASK query: " + e.getMessage(), e);
            }
        }
        else {
            try {
                TupleQuery query = connection.prepareTupleQuery(lang, queryText);
                query.setDataset(dataset);
                result = query.evaluate();
            }
            catch (Exception e) {
                throw new TrippiException("Exception while running Tuple (SELECT) query: " + e.getMessage(), e);
            }
        }
    }

    // ////////////////////////////////////////////////////////////////////////
    // ////////////// TupleIterator ///////////////////////////////////////////
    // ////////////////////////////////////////////////////////////////////////

    /**
     * Get the names of the binding variables.
     *
     * These will be the keys in the map for result.
     */
    @Override
    public String[] names() throws TrippiException {
        try {
            return result.getBindingNames().toArray(new String[0]);
        }
        catch (QueryEvaluationException e) {
            throw new TrippiException("Exception in names().", e);
        }
    }

    /**
     * Return true if there are any more tuples.
     */
    @Override
    public boolean hasNext() throws TrippiException {
        try {
            return result.hasNext();
        }
        catch (Exception e) {
            throw new TrippiException("Exception in hasNext().", e);
        }
    }

    /**
     * Return the next tuple.
     */
    @Override
    public Map<String, Node> next() throws TrippiException {
        Map<String, Node> to_return = new HashMap<String, Node>();
        try {
            for (Binding i : result.next()) {
                to_return.put(i.getName(), objectNode(i.getValue()));
            }
        }
        catch (Exception e) {
            throw new TrippiException("Exception in next().", e);
        }
        return to_return;
    }

    /**
     * Release resources held by this iterator.
     */
    @Override
    public void close() throws TrippiException {
        try {
            result.close();
        }
        catch (Exception e) {
            throw new TrippiException("Exception in close().", e);
        }
    }

    private ObjectNode objectNode(org.openrdf.model.Value object)
            throws GraphElementFactoryException, URISyntaxException {
        if (object == null)
            return null;
        if (object instanceof org.openrdf.model.URI) {
            return m_util.createResource(new URI(((org.openrdf.model.URI) object).toString()));
        }
        else if (object instanceof org.openrdf.model.Literal) {
            org.openrdf.model.Literal lit = (org.openrdf.model.Literal) object;
            org.openrdf.model.URI uri = lit.getDatatype();
            String lang = lit.getLanguage();
            if (uri != null) {
                // typed
                return m_util.createLiteral(lit.getLabel(), new URI(uri.toString()));
            }
            else if (lang != null && !lang.equals("")) {
                // local
                return m_util.createLiteral(lit.getLabel(), lang);
            }
            else {
                // plain
                return m_util.createLiteral(lit.getLabel());
            }
        }
        else {
            return m_util.createResource(((org.openrdf.model.BNode) object).getID().hashCode());
        }
    }

    private class BooleanBindingSet implements BindingSet {
        /**
         * 
         */
        private static final long serialVersionUID = 1L;
        private Map<String, Binding> map;

        public BooleanBindingSet(final boolean result) {
            map = new HashMap<String, Binding>();
            Binding binding = new Binding() {

                /**
                 * 
                 */
                private static final long serialVersionUID = 1L;

                @Override
                public Value getValue() {
                    ValueFactory vf = new ValueFactoryImpl();
                    return vf.createLiteral(result);
                }

                @Override
                public String getName() {
                    return "k0";
                }
            };
            map.put(binding.getName(), binding);
        }

        @Override
        public Binding getBinding(String arg0) {
            return map.get(arg0);
        }

        @Override
        public Set<String> getBindingNames() {
            return map.keySet();
        }

        @Override
        public Value getValue(String arg0) {
            return getBinding(arg0).getValue();
        }

        @Override
        public boolean hasBinding(String arg0) {
            return map.containsKey(arg0);
        }

        @Override
        public Iterator<Binding> iterator() {
            return map.values().iterator();
        }

        @Override
        public int size() {
            return map.size();
        }

    }

    private class BooleanTupleResult implements TupleQueryResult {
        private boolean served = false;
        private BooleanBindingSet result;

        public BooleanTupleResult(boolean result) {
            this.result = new BooleanBindingSet(result);
        }

        @Override
        public void close() throws QueryEvaluationException {
            // No-op.
        }

        @Override
        public boolean hasNext() throws QueryEvaluationException {
            return !served;
        }

        @Override
        public BindingSet next() throws QueryEvaluationException {
            if (!served) {
                served = true;
                return result;
            }
            throw new QueryEvaluationException("No more values.");
        }

        @Override
        public void remove() throws QueryEvaluationException {
            throw new QueryEvaluationException("Unsupported operation.");
        }

        @Override
        public List<String> getBindingNames() throws QueryEvaluationException {
            return new ArrayList<String>(result.getBindingNames());
        }

    }
}
