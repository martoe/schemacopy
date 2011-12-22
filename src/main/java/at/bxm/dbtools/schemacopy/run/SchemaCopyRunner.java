package at.bxm.dbtools.schemacopy.run;

import at.bxm.dbtools.schemacopy.table.CopyTargetMode;

import at.bxm.dbtools.schemacopy.Database;
import at.bxm.dbtools.schemacopy.Dialect;
import at.bxm.dbtools.schemacopy.SchemaCopyException;

import at.bxm.dbtools.schemacopy.sequence.SequenceAdjuster;

import at.bxm.dbtools.schemacopy.table.TableCopier;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public class SchemaCopyRunner {

	private static final Logger logger = LoggerFactory.getLogger(SchemaCopyRunner.class);
	private Database source;
	private Database target;
	private String csvDataResource;
	private String csvData;

	public void setSource(Database value) {
		source = value;
	}

	public void setTarget(Database value) {
		target = value;
	}

	public void setCsvData(String value) {
		csvData = value;
	}

	public void setCsvDataResource(String value) {
		csvDataResource = value;
	}

	public void copy(CopyTargetMode mode) {
		TableCopier tc = new TableCopier(source, target);
		SequenceAdjuster sa = new SequenceAdjuster(source, target);
		String line;
		BufferedReader in = null;
		try {
			in = new BufferedReader(getCsvDataReader());
			while ((line = in.readLine()) != null) {
				if (!line.startsWith("#")) {
					String[] tokens = line.split(";");
					tc.copy(tokens[0], source.getSchemaName(), null, target.getSchemaName(), mode);
					for (int i = 1; i < tokens.length; i++) {
						if (StringUtils.isNotBlank(tokens[i])) {
							sa.adjust(tokens[i], source.getSchemaName(), target.getSchemaName());
						}
					}
				}
			}
		} catch (IOException e) {
			throw new SchemaCopyException("Could not read CSV data", e);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException ignore) {
				}
			}
		}
	}

	public static void main(String[] args) throws IOException {
		String propertiesfile = args != null && args.length > 0 ? args[0] : "schemacopy.properties";
		Reader is = null;
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
			scr.setCsvDataResource(p.getProperty("datafile"));
			scr.copy(CopyTargetMode.valueOf(p.getProperty("copymode").toUpperCase()));
		} finally {
			if (is != null) {
				is.close();
			}
			if (datafile != null) {
				datafile.close();
			}
		}
	}

	private Reader getCsvDataReader() throws IOException {
		if (csvData != null) {
			return new StringReader(csvData);
		} else {
			return open(csvDataResource);
		}
	}

	private static Reader open(String resource) throws IOException {
		URL url = Thread.currentThread().getContextClassLoader().getResource(resource);
		if (url != null) {
			logger.debug("Using classpath resource " + url);
			return new InputStreamReader(url.openStream());
		} else {
			File file = new File(resource);
			if (file.exists() && file.isFile()) {
				logger.debug("Using file " + file.getAbsolutePath());
				return new FileReader(file);
			} else {
				throw new FileNotFoundException(resource);
			}
		}
	}

}