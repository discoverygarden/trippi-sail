package ca.discoverygarden.trippi.sesame.blazegraph;

import java.io.File;

import org.junit.After;
import org.springframework.test.context.ContextConfiguration;
import org.trippi.TrippiException;

import ca.discoverygarden.trippi.sesame.AbstractSesameConnectorIntegrationTest;

@ContextConfiguration
public class EmbeddedBlazegraphIntegrationTest extends AbstractSesameConnectorIntegrationTest {
	// Everything is done in the abstract base.
	
	@After
	public void tearDown() throws TrippiException {
		connector.close();
		File db = new File(System.getProperty("java.io.tmpdir"), "blazegraph-database.jnl");
		db.delete();
	}
}
