package ca.discoverygarden.trippi.sesame.blazegraph;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.Triple;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.repository.Repository;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.test.annotation.ExpectedException;
import org.springframework.test.context.ContextConfiguration;
import org.trippi.AliasManager;
import org.trippi.TrippiException;
import org.trippi.impl.sesame.ReadOnlySesameSession;
import org.trippi.impl.sesame.ReadOnlySessionException;

import ca.discoverygarden.trippi.sesame.AbstractSesameConnectorIntegrationTest;

@ContextConfiguration
public class RemoteBlazegraphIntegrationTest extends
AbstractSesameConnectorIntegrationTest implements ApplicationContextAware {
	protected ReadOnlySesameSession readOnlySession;
	protected Set<Triple> other_triples;

	@Override
	@Before
	public void setUp() throws Exception {
		assumeTrue(System.getProperties().containsKey("remoteBlazegraph"));
		super.setUp();
		readOnlySession = new ReadOnlySesameSession(
				context.getBean("trippiSailRepository", Repository.class),
				context.getBean("org.trippi.AliasManager", AliasManager.class), "test://model#", "ri");
		other_triples = new HashSet<Triple>();
		other_triples.add(geFactory.createTriple(geFactory.createResource(new URI("that://thing")),
				geFactory.createResource(new URI("does://that")), geFactory.createResource(new URI("crazy://thing"))));
	}

	@After
	public void tearDown() throws IOException, TrippiException {
		if (writer == null) {
			return;
		}
		for (Triple triple: other_triples) {
			writer.delete(triple, false);
		}
		writer.flushBuffer();
	}

	@Test
	public void testChanges() throws TrippiException, GraphElementFactoryException, URISyntaxException, IOException {
		// Get the initial count of a query.
		int pre_count = readOnlySession.query("SELECT * FROM <#ri> WHERE { ?s ?p ?o }", "sparql").count();
		// Add a triple.
		for (Triple triple: other_triples) {
			writer.add(triple, false);
		}
		writer.flushBuffer();
		// Ensure the read-only session can find the new triple(s).
		int post_count = readOnlySession.query("SELECT * FROM <#ri> WHERE { ?s ?p ?o }", "sparql").count();
		assertTrue("Read-only session picked up changes.", post_count > pre_count);
	}

	@Test
	@ExpectedException(ReadOnlySessionException.class)
	public void testDeleteReadOnly() throws TrippiException
	{
		readOnlySession.delete(other_triples);
	}

	@Test
	@ExpectedException(ReadOnlySessionException.class)
	public void testAddReadOnly() throws TrippiException {
		readOnlySession.add(other_triples);
	}

	protected ApplicationContext context;
	@Override
	public void setApplicationContext(ApplicationContext arg0) throws BeansException {
		// TODO Auto-generated method stub
		context = arg0;
	}
}
