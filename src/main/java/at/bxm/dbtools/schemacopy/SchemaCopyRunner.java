package at.bxm.dbtools.schemacopy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

// TODO sequence adjuster test
// TODO support dialect mixture
public class SchemaCopyRunner {

	private static final Logger logger = LoggerFactory.getLogger(SchemaCopyRunner.class);
	private Database source;
	private Database target;
	private BufferedReader csvData;

	public void setSource(Database value) {
		source = value;
	}

	public void setTarget(Database value) {
		target = value;
	}

	public void setCsvData(BufferedReader value) {
		csvData = value;
	}

	public void copy() throws IOException {
		TableCopier tc = new TableCopier();
		SequenceAdjuster sa = getSequenceAdjuster();
		tc.setSource(source.getDataSource());
		tc.setTarget(target.getDataSource());
		String line;
		while ((line = csvData.readLine()) != null) {
			if (!line.startsWith("#")) {
				String[] tokens = line.split(";");
				tc.copy(tokens[0], source.getSchemaName(), null, target.getSchemaName(), tokens[1]);
				for (int i = 2; i < tokens.length; i++) {
					sa.adjust(tokens[i], source.getSchemaName(), target.getSchemaName());
				}
			}
		}
	}

	public static void main(String[] args) throws IOException {
		String propertiesfile = args != null && args.length > 0 ? args[0] : "schemacopy.properties";
		InputStream is = null;
		InputStream datafile = null;
		try {
			is = open(propertiesfile);
			Properties p = new Properties();
			p.load(is);
			SchemaCopyRunner scr = new SchemaCopyRunner();
			scr.setSource(new Database(
				new DriverManagerDataSource(p.getProperty("source.url"),
					p.getProperty("source.username"), p.getProperty("source.password")),
				Dialect.valueOf(p.getProperty("source.dialect")),
				p.getProperty("source.schemaname")));
			scr.setTarget(new Database(
				new DriverManagerDataSource(p.getProperty("target.url"),
					p.getProperty("target.username"), p.getProperty("target.password")),
				Dialect.valueOf(p.getProperty("target.dialect")),
				p.getProperty("target.schemaname")));
			datafile = open(p.getProperty("datafile"));
			scr.setCsvData(new BufferedReader(new InputStreamReader(datafile)));
			scr.copy();
		} finally {
			if (is != null) {
				is.close();
			}
			if (datafile != null) {
				datafile.close();
			}
		}
	}

	private static InputStream open(String resource) throws IOException {
		URL url = Thread.currentThread().getContextClassLoader().getResource(resource);
		if (url != null) {
			logger.debug("Using classpath resource " + url);
			return url.openStream();
		} else {
			File file = new File(resource);
			if (file.exists() && file.isFile()) {
				logger.debug("Using file " + file.getAbsolutePath());
				return new FileInputStream(file);
			} else {
				throw new FileNotFoundException(resource);
			}
		}
	}

	private SequenceAdjuster getSequenceAdjuster() {
		if (source.getDialect() != target.getDialect()) {
			throw new SchemaCopyException("Dialect mismatch");
		}
		switch (source.getDialect()) {
		case ORACLE:
			return new OracleSequenceAdjuster();
		default: // H2
			return new H2SequenceAdjuster();
		}
	}

}
