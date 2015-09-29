package ca.discoverygarden.trippi.sesame.blazegraph;

import static org.junit.Assume.*;

import java.net.URISyntaxException;

import org.jrdf.graph.GraphElementFactoryException;
import org.junit.Before;
import org.springframework.test.context.ContextConfiguration;
import org.trippi.TrippiException;

import ca.discoverygarden.trippi.sesame.AbstractSesameConnectorIntegrationTest;

@ContextConfiguration
public class RemoteBlazegraphIntegrationTest extends
		AbstractSesameConnectorIntegrationTest {
	@Before
	public void setUp() throws TrippiException, GraphElementFactoryException,
			URISyntaxException {
		assumeTrue("We have a remote Blazegraph with which to test.", System
				.getProperties().containsKey("remoteBlazegraph"));
		super.setUp();
	}
}
