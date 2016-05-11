package org.trippi.impl.sesame;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jrdf.graph.BlankNode;
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
import org.openrdf.query.Dataset;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.Query;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.springframework.beans.factory.annotation.Configurable;
import org.trippi.Alias;
import org.trippi.AliasManager;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;
import org.trippi.TupleIterator;
import org.trippi.impl.base.DefaultAliasManager;

/**
 * A <code>TriplestoreSession</code> that wraps a SesameRepository.
 *
 * @author cwilper@cs.cornell.edu
 */
@Configurable
public class SesameSession implements AliasManagedTriplestoreSession {
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

	private boolean m_closed;

	@Override
	public String[] listTupleLanguages() {
		return TUPLE_LANGUAGES;
	}

	@Override
	public String[] listTripleLanguages() {
		return TRIPLE_LANGUAGES;
	}

	private String serverUri;
	private String model;

	public SesameSession(Repository repository, String serverUri, String model)
			throws RepositoryException {
		this(repository, new DefaultAliasManager(), serverUri, model);
	}

	/**
	 * Construct a Sesame session.
	 *
	 * @throws RepositoryException
	 */
	public SesameSession(Repository repository, AliasManager aliasManager,
			String serverUri, String model) throws RepositoryException {
		m_repository = repository;
		if (!repository.isInitialized()) {
			repository.initialize();
		}
		connection = repository.getConnection();
		m_aliasManager = aliasManager;
		this.serverUri = serverUri;
		this.model = model;
		m_closed = false;
	}

	private Dataset ds;

	private Dataset getDataset() throws RepositoryException {
		if (ds == null) {
			RepositoryResult<Resource> graphs = connection.getContextIDs();
			Set<org.openrdf.model.URI> ngs = new HashSet<org.openrdf.model.URI>();
			while (graphs.hasNext()) {
				Resource g = graphs.next();
				if (g instanceof org.openrdf.model.URI) {
					ngs.add((org.openrdf.model.URI) g);
				}
			}
			DatasetImpl ds = new DatasetImpl();
			for (org.openrdf.model.URI g : ngs) {
				ds.addDefaultGraph(g);
			}
			this.ds = ds;
		}
		return ds;
	}

	@Override
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

	private void doTriples(Iterable<Triple> triples, boolean add)
			throws TrippiException {
		try {
			ValueFactory valueFactory = ValueFactoryImpl.getInstance();
			Resource model = valueFactory.createURI(serverUri, this.model);
			try {
				connection.begin();
				if (add) {
					connection.add(getSesameGraph(triples, valueFactory), model);
				} else {
					connection.remove(getSesameGraph(triples, valueFactory), model);
				}
				connection.commit();
			} catch (Exception e) {
				connection.rollback();
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
		for (Alias alias : getAliasManager().getAliases().values()) {
			out = alias.replaceSparqlType(alias.replaceSparqlUri(out));
		}
		// base model URI includes separator
		out = Alias.replaceRelativeUris(out, serverUri);
		System.out.println("Query: " + out);

		return out;
	}

	private String doAliasReplacements(String q, boolean noBrackets) {
		String out = q;

		if (noBrackets) {
			for (Alias alias : getAliasManager().getAliases().values()) {
				// In serql, aliases are not surrounded by < and >
				// If bob is an alias for http://example.org/robert/,
				// this turns bob:fun into <http://example.org/robert/fun>,
				// {bob:fun} into {<http://example.org/robert/fun>},
				// and "10"^^xsd:int into
				// "10"^^<http://www.w3.org/2001/XMLSchema#int>
				out = out.replaceAll("([\\s{\\^])" + alias.getKey()
				+ ":([^\\s}]+)", "$1<" + alias.getExpansion() + "$2>");
			}
		} else {
			out = doAliasReplacements(q);
		}

		return out;
	}

	@Override
	public TripleIterator findTriples(SubjectNode subject,
			PredicateNode predicate, ObjectNode object) throws TrippiException {
		GraphQuery query;
		try {
			query = connection.prepareGraphQuery(QueryLanguage.SPARQL,
					"CONSTRUCT WHERE { ?s ?p ?o }");
		} catch (Exception e) {
			throw new TrippiException(e.getMessage());
		}

		Map<String, Node> map = new HashMap<String, Node>();
		map.put("s", subject);
		map.put("p", predicate);
		map.put("o", object);

		performNodeBindings(map, query);

		try {
			return new SesameTripleIterator(query, getDataset());
		} catch (RepositoryException e) {
			throw new TrippiException(
					"Error instanciating SesameTripleIterator.", e);
		}
	}

	private void performNodeBindings(final Map<String, Node> bindings,
			final Query query) {
		ValueFactory valueFactory = ValueFactoryImpl.getInstance();
		for (String i : bindings.keySet()) {
			Node n = bindings.get(i);
			if (n != null) {
				query.setBinding(i, getSesameValue(n, valueFactory));
			}
		}
	}

	@Override
	public TripleIterator findTriples(String language, String queryText)
			throws TrippiException {
		QueryLanguage lang = languageMap.get(language.toLowerCase());

		if (lang == null) {
			throw new TrippiException("Unsupported query language: " + language);
		}

		try {
			return new SesameTripleIterator(lang, doAliasReplacements(
					queryText, lang == QueryLanguage.SERQL), connection,
					getDataset());
		} catch (Exception e) {
			throw new TrippiException(e.getMessage());
		}
	}

	@Override
	public TupleIterator query(String queryText, String language)
			throws TrippiException {
		QueryLanguage lang = languageMap.get(language.toLowerCase());
		if (lang == null) {
			throw new TrippiException("Unsupported query language: " + language);
		}

		queryText = doAliasReplacements(queryText, lang == QueryLanguage.SERQL);

		try {
			return new SesameTupleIterator(lang, queryText, connection,
					getDataset());
		} catch (RepositoryException e) {
			throw new TrippiException(
					"Error instantiating SesameTupleIterator.", e);
		}
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
			Iterable<Triple> jrdfTriples, ValueFactory valueFactory) {
		@SuppressWarnings("deprecation")
		org.openrdf.model.Graph graph = new org.openrdf.model.impl.GraphImpl();

		for (Triple triple : jrdfTriples) {
			graph.add(getSesameStatement(triple, valueFactory));
		}
		return graph;
	}

	public static org.openrdf.model.Statement getSesameStatement(Triple triple,
			ValueFactory valueFactory) {
		return valueFactory
				.createStatement(
						getSesameResource(triple.getSubject(), valueFactory),
						getSesameURI((URIReference) triple.getPredicate(),
								valueFactory),
						getSesameValue(triple.getObject(), valueFactory));
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

	public static org.openrdf.model.Value getSesameValue(Node object,
			ValueFactory valueFactory) {
		if (object instanceof BlankNode) {
			return valueFactory.createBNode(String.valueOf(object.hashCode()));
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
