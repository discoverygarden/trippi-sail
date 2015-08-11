package ca.discoverygarden.trippi;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;

import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;

/**
 * Just verifying that it is actually possible to talk to a remote repo... Not a real test.
 * @author adam
 *
 */
public class BigdataRemoteRepoIntegrationTest {
	private RemoteRepositoryManager manager;
	private Repository repo;
	private RepositoryConnection connection;
	private ValueFactory valueFactory;
	private Statement statement;

	@Before
	public void setUp() throws Exception {
		manager = new RemoteRepositoryManager("http://whip-vagrant-i7latest.local:8080/bigdata", false);
		RemoteRepository asdf = manager.getRepositoryForDefaultNamespace();
		repo = new BigdataSailRemoteRepository(asdf);
		connection = repo.getConnection();
		valueFactory = new ValueFactoryImpl();
		statement = valueFactory.createStatement(
				valueFactory.createURI("http://what#thing"),
				valueFactory.createURI("http://that#thing"),
				valueFactory.createLiteral("Lol, string!"));
		connection.add(statement);
	}
	
	@After
	public void tearDown() throws Exception {
		connection.remove(statement);
	}

	@Test
	public void testTuple() throws Exception {
		TupleQuery query = connection.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT ?s ?p ?o WHERE {?s ?p ?o}");
		TupleQueryResult result = query.evaluate();
		if (!result.hasNext()) {
			fail("No results from TupleQuery.");
		}
	}

	@Test
	public void testGraph() throws Exception {
		GraphQuery query = connection.prepareGraphQuery(QueryLanguage.SPARQL, "CONSTRUCT {?s ?p ?o} WHERE {?s ?p ?o}");
		GraphQueryResult result = query.evaluate();
		if (!result.hasNext()) {
			fail("No results from TupleQuery.");
		}
	}
}
