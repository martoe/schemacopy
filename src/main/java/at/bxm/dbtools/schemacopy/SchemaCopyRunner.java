package at.bxm.dbtools.schemacopy;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
			String[] tokens = line.split(";");
			tc.copy(tokens[0], null, null, null, tokens[1]);
		}
	}

	public static void main(String[] args) throws IOException {
		Properties p = new Properties();
		URL url = Thread.currentThread().getContextClassLoader().getResource("schemacopy.properties");
		if (url != null) {
			logger.debug("Reading properties from " + url);
			p.load(url.openStream());
			logger.debug(p.toString()); // FIXME implement
		} else {
			throw new IllegalArgumentException("schemacopy.properties not found on classpath");
		}
	}

}
