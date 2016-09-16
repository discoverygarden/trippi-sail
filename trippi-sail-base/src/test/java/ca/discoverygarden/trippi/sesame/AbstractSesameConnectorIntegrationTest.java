package ca.discoverygarden.trippi.sesame;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jrdf.graph.GraphElementFactory;
import org.jrdf.graph.Triple;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.trippi.TripleIterator;
import org.trippi.TriplestoreWriter;
import org.trippi.TrippiException;
import org.trippi.impl.sesame.SesameConnector;

@TestExecutionListeners({ DependencyInjectionTestExecutionListener.class })
@RunWith(SpringJUnit4ClassRunner.class)
abstract public class AbstractSesameConnectorIntegrationTest {
    @Autowired
    protected SesameConnector connector;
    protected TriplestoreWriter writer;
    protected List<Triple> triples;
    protected GraphElementFactory geFactory;
    private org.slf4j.Logger logger;

    @Before
    public void setUp() throws Exception {
        writer = connector.getWriter();
        geFactory = connector.getElementFactory();

        triples = new ArrayList<Triple>();
        triples.add(geFactory.createTriple(
                geFactory.createResource(new URI("this://thing")),
                geFactory.createResource(new URI("has://that")),
                geFactory.createResource(new URI("crazy://property"))));
        logger = LoggerFactory.getLogger(getClass());
    }

    protected int getPresentCount(Triple i) throws TrippiException {
        TripleIterator tripit = writer.findTriples(i.getSubject(),
                i.getPredicate(), i.getObject(), 100);
        return tripit.count();
    }

    @Test
    public void test() throws IOException, TrippiException {
        Map<String, String> aliases = writer.getAliasMap();
        logger.info(String.format("Count: %s", aliases.size()));
        for (String key : aliases.keySet()) {
            logger.info(String.format("%s %s", key, aliases.get(key)));
        }
        writer.add(triples, true);
        for (Triple i : triples) {
            assertTrue("Added.", getPresentCount(i) > 0);
        }
        testGraphRewrite();
        writer.delete(triples, true);
        for (Triple i : triples) {
            assertTrue("Deleted.", getPresentCount(i) == 0);
        }
    }

    protected void testGraphRewrite() throws TrippiException {
        assertTrue(writer.findTuples("sparql",
                "SELECT * FROM <#ri> WHERE { ?s <has://that> ?o }", 100, true)
                .count() > 0);
    }
}
