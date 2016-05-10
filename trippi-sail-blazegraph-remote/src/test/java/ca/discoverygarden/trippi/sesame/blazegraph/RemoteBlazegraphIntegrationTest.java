package ca.discoverygarden.trippi.sesame.blazegraph;

import static org.junit.Assume.assumeTrue;

import java.net.URISyntaxException;

import org.jrdf.graph.GraphElementFactoryException;
import org.junit.Before;
import org.springframework.test.context.ContextConfiguration;
import org.trippi.TrippiException;

import ca.discoverygarden.trippi.sesame.AbstractSesameConnectorIntegrationTest;

@ContextConfiguration
public class RemoteBlazegraphIntegrationTest extends
AbstractSesameConnectorIntegrationTest {
	@Override
	@Before
	public void setUp() throws TrippiException, GraphElementFactoryException,
	URISyntaxException {
		assumeTrue(System.getProperties().containsKey("remoteBlazegraph"));
		super.setUp();
	}
}
