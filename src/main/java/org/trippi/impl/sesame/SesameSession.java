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
		for (QueryLanguage i : QueryLanguage.values()) {
			languageMap.put(i.getName().toLowerCase(), i);
		}
	}

	public static final String[] TUPLE_LANGUAGES = languageMap.keySet()
			.toArray(new String[0]);
	public static final String[] TRIPLE_LANGUAGES = languageMap.keySet()
			.toArray(new String[0]);

	private Repository m_repository;
	private RepositoryConnection connection;
	private AliasManager m_aliasManager;

	private GraphElementFactory m_factory;

	private boolean m_closed;

	@Override
	public String[] listTupleLanguages() {
		return TUPLE_LANGUAGES;
	}

	@Override
	public String[] listTripleLanguages() {
		return TRIPLE_LANGUAGES;
	}

	private String modelUri;
	
	public SesameSession(Repository repository, String modelUri) throws RepositoryException {
		this(repository, new DefaultAliasManager(), modelUri);
	}

	/**
	 * Construct a Sesame session.
	 * @throws RepositoryException 
	 */
	public SesameSession(Repository repository, AliasManager aliasManager, String modelUri) throws RepositoryException {
		m_repository = repository;
		connection = repository.getConnection();
		m_aliasManager = aliasManager;
		this.modelUri = modelUri;
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

	@Override
	public void add(Set<Triple> triples) throws TrippiException {
		doTriples(triples, true);
	}

	@Override
	public void delete(Set<Triple> triples) throws TrippiException {
		doTriples(triples, false);
	}

	private void doTriples(Set<Triple> triples, boolean add)
			throws TrippiException {
		try {
			ValueFactory valueFactory = new ValueFactoryImpl();
			Resource model = valueFactory.createURI(modelUri);
			if (add) {
				connection.add(getSesameGraph(triples, valueFactory, model));
			} else {
				connection.remove(getSesameGraph(triples, valueFactory, model));
			}
		} catch (Exception e) {
			e.printStackTrace();
			String mod = "deleting";
			if (add)
				mod = "adding";
			String msg = "Error " + mod + " triples: " + e.getClass().getName();
			if (e.getMessage() != null)
				msg = msg + ": " + e.getMessage();
			throw new TrippiException(msg, e);
		}
	}
	
	/**
	 * 
	 * @see org.trippi.impl.mulgara.MulgaraSession.doAliasReplacements()
	 * @param q
	 * @return
	 */
	private String doAliasReplacements(String q) {
		String out = q;
		for (Alias alias: getAliasManager().getAliases().values()) {
			out = alias.replaceSparqlType(alias.replaceSparqlUri(out));
		}
		// base model URI includes separator
		// relative URIs introduce a library dependency on Jena, so keeping m_serverURI for now 
		out = Alias.replaceRelativeUris(out, modelUri); 

		return out;
	}

	private String doAliasReplacements(String q, boolean noBrackets) {
		String out = q;

		if (noBrackets) {
			for (Alias alias: getAliasManager().getAliases().values()) {
				// In serql, aliases are not surrounded by < and >
				// If bob is an alias for http://example.org/robert/,
				// this turns bob:fun into <http://example.org/robert/fun>,
				// {bob:fun} into {<http://example.org/robert/fun>},
				// and "10"^^xsd:int into
				// "10"^^<http://www.w3.org/2001/XMLSchema#int>
				out = out.replaceAll("([\\s{\\^])" + alias.getKey() + ":([^\\s}]+)",
						"$1<" + alias.getExpansion() + "$2>");
			}
		}
		else {
			out = doAliasReplacements(q);
		}

		return out;
	}

	@Override
	public TripleIterator findTriples(SubjectNode subject, PredicateNode predicate, ObjectNode object) throws TrippiException {
		final String query = String.format("CONSTRUCT WHERE {%s %s %s}",
				getString("?s", subject),
				getString("?p", predicate),
				getString("?o", object));

		return findTriples(QueryLanguage.SPARQL.getName(), query);
	}

	private String getString(String ifNull, Node rdfNode) {
		if (rdfNode == null)
			return ifNull;
		return RDFUtil.toString(rdfNode);
	}

	@Override
	public TripleIterator findTriples(String language, String queryText) throws TrippiException {
		QueryLanguage lang = languageMap.get(language.toLowerCase());

		if (lang == null) {
			throw new TrippiException("Unsupported query language: " + language);
		}

		return new SesameTripleIterator(lang, doAliasReplacements(queryText, lang == QueryLanguage.SERQL), connection);
	}

	@Override
	public TupleIterator query(String queryText, String language) throws TrippiException {
		QueryLanguage lang = languageMap.get(language.toLowerCase());
		if (lang == null) {
			throw new TrippiException("Unsupported query language: " + language);
		}

		queryText = doAliasReplacements(queryText, lang == QueryLanguage.SERQL);

		return new SesameTupleIterator(lang, queryText, connection);
	}

	@Override
	public void close() throws TrippiException {
		if (!m_closed) {
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
	@Override
	public void finalize() throws TrippiException {
		close();
	}

	// ////////////////////////////////////////////////////////////////////////
	// ////// Utility methods for converting JRDF types to Sesame types ///////
	// ////////////////////////////////////////////////////////////////////////

	public static org.openrdf.model.Graph getSesameGraph(
			Iterable<Triple> jrdfTriples, ValueFactory valueFactory, Resource model) {
		org.openrdf.model.Graph graph = new org.openrdf.model.impl.TreeModel();

		for (Triple triple : jrdfTriples) {
			graph.add(getSesameStatement(triple, valueFactory, model));
		}
		return graph;
	}

	public static org.openrdf.model.Statement getSesameStatement(Triple triple,
			ValueFactory valueFactory, Resource model) {
		return valueFactory.createStatement(
				getSesameResource(triple.getSubject(), valueFactory),
				getSesameURI((URIReference) triple.getPredicate(), valueFactory),
				getSesameValue(triple.getObject(), valueFactory),
				model);
	}

	public static org.openrdf.model.Resource getSesameResource(
			SubjectNode subject, ValueFactory valueFactory) {
		if (subject instanceof BlankNode) {
			return valueFactory.createBNode("" + subject.hashCode());
		} else { // must be a URIReference
			return getSesameURI((URIReference) subject, valueFactory);
		}
	}

	public static org.openrdf.model.URI getSesameURI(URIReference uriReference,
			ValueFactory valueFactory) {
		return valueFactory.createURI(uriReference.getURI().toString());
	}

	public static org.openrdf.model.Value getSesameValue(ObjectNode object,
			ValueFactory valueFactory) {
		if (object instanceof BlankNode) {
			return valueFactory.createBNode("" + object.hashCode());
		} else if (object instanceof URIReference) {
			return getSesameURI((URIReference) object, valueFactory);
		} else { // must be a Literal
			return getSesameLiteral((Literal) object, valueFactory);
		}
	}

	public static org.openrdf.model.Literal getSesameLiteral(Literal literal,
			ValueFactory valueFactory) {
		String value = literal.getLexicalForm();
		String lang = literal.getLanguage();
		URI type = literal.getDatatypeURI();
		if (lang != null) {
			return valueFactory.createLiteral(value, lang);
		} else if (type != null) {
			return valueFactory.createLiteral(value,
					valueFactory.createURI(type.toString()));
		} else {
			return valueFactory.createLiteral(value);
		}
	}

}
