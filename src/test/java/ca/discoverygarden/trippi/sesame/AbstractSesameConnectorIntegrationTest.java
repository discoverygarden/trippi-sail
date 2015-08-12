package ca.discoverygarden.trippi.sesame;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.jrdf.graph.GraphElementFactoryException;
import org.jrdf.graph.Triple;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.trippi.RDFUtil;
import org.trippi.TripleIterator;
import org.trippi.TriplestoreWriter;
import org.trippi.TrippiException;
import org.trippi.impl.sesame.SesameConnector;

@RunWith(SpringJUnit4ClassRunner.class)
abstract public class AbstractSesameConnectorIntegrationTest {
	@Autowired
	protected SesameConnector connector;
	protected TriplestoreWriter writer;
	protected List<Triple> triples;
	protected RDFUtil geFactory;
	
	@Before
	public void setUp() throws TrippiException, GraphElementFactoryException, URISyntaxException {
		writer = connector.getWriter();
		
		geFactory = new RDFUtil();
		
		triples = new ArrayList<Triple>();
		triples.add(geFactory.createTriple(
				geFactory.createResource(new URI("this://thing")),
				geFactory.createResource(new URI("has://that")),
				geFactory.createResource(new URI("crazy://property"))
				));
	}
	
	private int getPresentCount(Triple i) throws TrippiException {
			TripleIterator tripit = writer.findTriples(i.getSubject(), i.getPredicate(), i.getObject(), 100);
			return tripit.count();
	}
	
	@Test
	public void test() throws IOException, TrippiException {
		writer.add(triples, true);
		for (Triple i: triples) {
			assertTrue("Added.", getPresentCount(i) > 0);
		}
		writer.delete(triples, true);
		for (Triple i: triples) {
			assertTrue("Deleted.", getPresentCount(i) == 0);
		}
	}
}
