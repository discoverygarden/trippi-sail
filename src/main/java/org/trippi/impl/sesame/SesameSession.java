package org.trippi.impl.sesame;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jrdf.graph.BlankNode;
import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.Literal;
import org.jrdf.graph.Node;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.jrdf.graph.URIReference;
import org.openrdf.model.Resource;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.trippi.Alias;
import org.trippi.RDFUtil;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;
import org.trippi.AliasManager;
import org.trippi.impl.base.DefaultAliasManager;
import org.trippi.impl.base.TriplestoreSession;

/**
 * A <code>TriplestoreSession</code> that wraps a SesameRepository.
 *
 * @author cwilper@cs.cornell.edu
 */
public class SesameSession implements TriplestoreSession {
    public static final Map<String, QueryLanguage> languageMap = new HashMap<String, QueryLanguage>();
    static {
    	for (QueryLanguage i: QueryLanguage.values()) {
    		languageMap.put(i.getName().toLowerCase(), i);
    	}
    }

    public static final String[] TUPLE_LANGUAGES = languageMap.keySet().toArray(new String[0]);
    public static final String[] TRIPLE_LANGUAGES = languageMap.keySet().toArray(new String[0]);

    private Repository m_repository;
    private RepositoryConnection connection;
    private AliasManager m_aliasManager;

    private GraphElementFactory m_factory;

    private boolean m_closed;

    public String[] listTupleLanguages() {
        return TUPLE_LANGUAGES;
    }

    public String[] listTripleLanguages() {
        return TRIPLE_LANGUAGES;
    }

    public SesameSession(Repository repository) {
    	this(repository, new DefaultAliasManager());
    }
    
    /**
     * Construct a Sesame session.
     */
    public SesameSession(Repository repository,
                         AliasManager aliasManager) {
        m_repository = repository;
        try {
			connection = repository.getConnection();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        m_aliasManager = aliasManager;
        m_closed = false;
    }

    public GraphElementFactory getElementFactory() {
        if (m_factory == null) {
            m_factory = new RDFUtil();
        }
        return m_factory;
    }

    public AliasManager getAliasManager() {
    	return m_aliasManager;
    }

    public void add(Set<Triple> triples) throws TrippiException {
        doTriples(triples, true);
    }

    public void delete(Set<Triple> triples) throws TrippiException {
        doTriples(triples, false);
    }

    private void doTriples(Set<Triple> triples, 
                           boolean add) throws TrippiException {
        try { 
            ValueFactory valueFactory = new ValueFactoryImpl();
            Resource graph = valueFactory.createURI("http://localhost/ri");
            if (add) {
            	connection.add(getSesameGraph(triples, valueFactory), graph);
            } else {
                connection.remove(getSesameGraph(triples, valueFactory), graph);
            }
        } catch (Exception e) {
            e.printStackTrace();
            String mod = "deleting";
            if (add) mod = "adding";
            String msg = "Error " + mod + " triples: " + e.getClass().getName();
            if (e.getMessage() != null) msg = msg + ": " + e.getMessage();
            throw new TrippiException(msg, e);
        }
    }

    private String doAliasReplacements(String q, boolean noBrackets) {
        String out = q;
        Map<String, Alias> m = m_aliasManager.getAliases();
        for (String alias: m.keySet()) {
            String fullForm = m.get(alias).getExpansion();
            if (noBrackets) {
                // In serql and rql, aliases are not surrounded by < and >
                // If bob is an alias for http://example.org/robert/,
                // this turns bob:fun into <http://example.org/robert/fun>,
                // {bob:fun} into {<http://example.org/robert/fun>},
                // and "10"^^xsd:int into "10"^^<http://www.w3.org/2001/XMLSchema#int>
                out = out.replaceAll("([\\s{\\^])" + alias + ":([^\\s}]+)",
                                     "$1<" + fullForm + "$2>");
            } else {
                // In other query languages, aliases are surrounded by < and >
                // If bob is an alias for http://example.org/robert/,
                // this turns <bob:fun> into <http://example.org/robert/fun>
                // and "10"^^xsd:int into "10"^^<http://www.w3.org/2001/XMLSchema#int>
                out = out.replaceAll("<" + alias + ":", "<" + fullForm)
                         .replaceAll("\\^\\^" + alias + ":(\\S+)", "^^<" + fullForm + "$1>");
            }
        }

        return out;
    }

    public TripleIterator findTriples(SubjectNode subject,
                                      PredicateNode predicate,
                                      ObjectNode object) throws TrippiException {
        // convert the pattern to a SERQL CONSTRUCT query and run that
        StringBuffer buf = new StringBuffer();
        buf.append("CONSTRUCT * FROM {S} " + getString("P", predicate) + " {O}");

        if (subject != null || object != null) {
            buf.append(" WHERE ");
            if (subject != null) {
                buf.append("S = " + getString(null, subject));
                if (object != null) buf.append(" AND ");
            }
            if (object != null) {
                buf.append("O = " + getString(null, object));
            }
        }

        return findTriples(QueryLanguage.SERQL.getName().toLowerCase(), buf.toString());
    }

    private String getString(String ifNull, Node rdfNode) {
        if (rdfNode == null) return ifNull;
        return RDFUtil.toString(rdfNode);
    }

    public TripleIterator findTriples(String language,
                                      String queryText) throws TrippiException {
    	QueryLanguage lang = languageMap.get(language.toLowerCase());
    	
        if (lang == null) {
            throw new TrippiException("Unsupported query language: " + language);
        }
        
        return new SesameTripleIterator(
        		lang, 
        		doAliasReplacements(queryText, lang == QueryLanguage.SERQL), 
        		connection);
    }

    public TupleIterator query(String queryText,
                               String language) throws TrippiException {
        QueryLanguage lang = languageMap.get(language.toLowerCase());
        if (lang == null) {
            throw new TrippiException("Unsupported query language: " + language);
        }
        
        queryText = doAliasReplacements(queryText, lang == QueryLanguage.SERQL);

        return new SesameTupleIterator(lang, queryText, connection);
    }

    public void close() throws TrippiException {
        if ( !m_closed ) {
            try {
            	connection.close();
				m_repository.shutDown();
			} catch (RepositoryException e) {
				throw new TrippiException("Exception in close().", e);
			}
            m_closed = true;
        }
    }

    /**
     * Ensure close() gets called at garbage collection time.
     */
    public void finalize() throws TrippiException {
        close();
    }

    //////////////////////////////////////////////////////////////////////////
    //////// Utility methods for converting JRDF types to Sesame types ///////
    //////////////////////////////////////////////////////////////////////////

	public static org.openrdf.model.Graph getSesameGraph(Iterable<Triple> jrdfTriples, ValueFactory valueFactory) {
        org.openrdf.model.Graph graph = new org.openrdf.model.impl.TreeModel();

        for (Triple triple: jrdfTriples) {
            graph.add(getSesameStatement(triple, valueFactory));
        }
        return graph;
    }

    public static org.openrdf.model.Statement getSesameStatement(
            Triple triple, 
            ValueFactory valueFactory) {
        return new org.openrdf.model.impl.StatementImpl(
                    getSesameResource(triple.getSubject(), valueFactory),
                    getSesameURI((URIReference) triple.getPredicate(), valueFactory),
                    getSesameValue(triple.getObject(), valueFactory));
    }

    public static org.openrdf.model.Resource getSesameResource(
            SubjectNode subject,
            ValueFactory valueFactory) {
        if (subject instanceof BlankNode) {
            return valueFactory.createBNode("" + subject.hashCode());
        } else {  // must be a URIReference
            return getSesameURI((URIReference) subject, valueFactory);
        }
    }

    public static org.openrdf.model.URI getSesameURI(
            URIReference uriReference,
            ValueFactory valueFactory) {
        return valueFactory.createURI(uriReference.getURI().toString());
    }

    public static org.openrdf.model.Value getSesameValue(
            ObjectNode object,
            ValueFactory valueFactory) {
        if (object instanceof BlankNode) {
            return valueFactory.createBNode("" + object.hashCode());
        } else if (object instanceof URIReference) {
            return getSesameURI((URIReference) object, valueFactory);
        } else { // must be a Literal
            return getSesameLiteral((Literal) object, valueFactory);
        }
    }

    public static org.openrdf.model.Literal getSesameLiteral(
            Literal literal,
            ValueFactory valueFactory) {
        String value = literal.getLexicalForm();
        String lang  = literal.getLanguage();
        URI    type  = literal.getDatatypeURI(); 
        if (lang != null) {
            return valueFactory.createLiteral(value, lang);
        } else if (type != null) {
            return valueFactory.createLiteral(value, 
                                              valueFactory.createURI(
                                                           type.toString()));
        } else {
            return valueFactory.createLiteral(value);
        }
    }

}
