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
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public class SchemaCopyRunner {

	private static final Logger logger = LoggerFactory.getLogger(SchemaCopyRunner.class);
	private DataSource source;
	private DataSource target;
	private BufferedReader csvData;

	public void setSource(DataSource value) {
		source = value;
	}

	public void setTarget(DataSource value) {
		target = value;
	}

	public void setCsvData(BufferedReader value) {
		csvData = value;
	}

	public void run() throws IOException {
		TableCopier tc = new TableCopier();
		tc.setSource(source);
		tc.setTarget(target);
		String line;
		while ((line = csvData.readLine()) != null) {
			if (!line.startsWith("#")) {
				String[] tokens = line.split(";");
				tc.copy(tokens[0], null, null, null, tokens[1]);
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
			scr.setSource(new DriverManagerDataSource(p.getProperty("sourcedb.url"),
				p.getProperty("sourcedb.username"), p.getProperty("sourcedb.password")));
			scr.setTarget(new DriverManagerDataSource(p.getProperty("targetdb.url"),
				p.getProperty("targetdb.username"), p.getProperty("targetdb.password")));
			datafile = open(p.getProperty("datafile"));
			scr.setCsvData(new BufferedReader(new InputStreamReader(datafile)));
			scr.run();
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

}
