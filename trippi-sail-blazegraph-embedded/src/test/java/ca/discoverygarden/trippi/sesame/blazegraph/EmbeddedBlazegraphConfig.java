package ca.discoverygarden.trippi.sesame.blazegraph;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import com.bigdata.rdf.sail.BigdataSail;

@Configuration
public class EmbeddedBlazegraphConfig {
	@Bean
	@Scope("prototype")
	public BigdataSail blazegraphSail() throws IOException {
		File tempfile = File.createTempFile("bigdata", ".jnl");
		tempfile.deleteOnExit();
		Properties prop = new Properties();
		prop.load(this.getClass().getResourceAsStream("quads.properties"));
		prop.put("com.bigdata.journal.AbstractJournal.file",
				tempfile.getAbsolutePath());
		return new BigdataSail(prop);
	}

}
