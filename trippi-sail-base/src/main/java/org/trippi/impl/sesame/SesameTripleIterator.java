package org.trippi.impl.sesame;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.ObjectNode;
import org.jrdf.graph.PredicateNode;
import org.jrdf.graph.SubjectNode;
import org.jrdf.graph.Triple;
import org.openrdf.model.Statement;
import org.openrdf.query.Dataset;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.trippi.RDFUtil;
import org.trippi.TripleIterator;
import org.trippi.TrippiException;

public class SesameTripleIterator extends TripleIterator {
	private RDFUtil m_util;

	private GraphQueryResult result;

	public SesameTripleIterator(QueryLanguage lang, String queryText,
			RepositoryConnection connection, Dataset dataset)
			throws TrippiException, RepositoryException,
			MalformedQueryException {
		this(connection.prepareGraphQuery(lang, queryText), dataset);
	}

	public SesameTripleIterator(GraphQuery query, Dataset dataset)
			throws TrippiException {
		m_util = new RDFUtil();

		try {
			query.setDataset(dataset);
			result = query.evaluate();
		} catch (QueryEvaluationException e) {
			throw new TrippiException("Exception when running query: "
					+ e.getMessage());
		}
	}

	// ////////////////////////////////////////////////////////////////////////
	// ////////////// TripleIterator //////////////////////////////////////////
	// ////////////////////////////////////////////////////////////////////////

	/**
	 * Return true if there are any more triples.
	 */
	@Override
	public boolean hasNext() throws TrippiException {
		try {
			return result.hasNext();
		} catch (QueryEvaluationException e) {
			throw new TrippiException("Exception in hasNext().", e);
		}
	}

	/**
	 * Return the next triple.
	 */
	@Override
	public Triple next() throws TrippiException {
		try {
			Statement next;
			next = result.next();
			return triple(next.getSubject(), next.getPredicate(),
					next.getObject());
		} catch (Exception e) {
			throw new TrippiException("Exception in next().", e);
		}
	}

	/**
	 * Release resources held by this iterator.
	 */
	@Override
	public void close() throws TrippiException {
		try {
			result.close();
		} catch (Exception e) {
			throw new TrippiException("Exception in close().", e);
		}
	}

	private Triple triple(org.openrdf.model.Resource subject,
			org.openrdf.model.URI predicate, org.openrdf.model.Value object)
			throws IOException {
		try {
			return m_util.createTriple(subjectNode(subject),
					predicateNode(predicate), objectNode(object));
		} catch (Exception e) {
			String msg = e.getClass().getName();
			if (e.getMessage() != null)
				msg += ": " + e.getMessage();
			e.printStackTrace();
			throw new IOException(
					"Error converting Sesame (Resource,URI,Value) to JRDF Triple: "
							+ msg);
		}
	}

	private SubjectNode subjectNode(org.openrdf.model.Resource subject)
			throws GraphElementFactoryException, URISyntaxException {
		if (subject instanceof org.openrdf.model.URI) {
			return m_util.createResource(new URI(
					((org.openrdf.model.URI) subject).toString()));
		} else {
			return m_util.createResource(((org.openrdf.model.BNode) subject)
					.getID().hashCode());
		}
	}

	private PredicateNode predicateNode(org.openrdf.model.URI predicate)
			throws GraphElementFactoryException, URISyntaxException {
		return m_util.createResource(new URI(predicate.toString()));
	}

	private ObjectNode objectNode(org.openrdf.model.Value object)
			throws GraphElementFactoryException, URISyntaxException {
		if (object instanceof org.openrdf.model.URI) {
			return m_util.createResource(new URI(
					((org.openrdf.model.URI) object).toString()));
		} else if (object instanceof org.openrdf.model.Literal) {
			org.openrdf.model.Literal lit = (org.openrdf.model.Literal) object;
			org.openrdf.model.URI uri = lit.getDatatype();
			String lang = lit.getLanguage();
			if (uri != null) {
				// typed
				return m_util.createLiteral(lit.getLabel(),
						new URI(uri.toString()));
			} else if (lang != null && !lang.equals("")) {
				// local
				return m_util.createLiteral(lit.getLabel(), lang);
			} else {
				// plain
				return m_util.createLiteral(lit.getLabel());
			}
		} else {
			return m_util.createResource(((org.openrdf.model.BNode) object)
					.getID().hashCode());
		}
	}
}
