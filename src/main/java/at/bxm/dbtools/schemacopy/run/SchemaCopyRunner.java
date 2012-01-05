package at.bxm.dbtools.schemacopy.run;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import at.bxm.dbtools.schemacopy.Database;
import at.bxm.dbtools.schemacopy.Dialect;
import at.bxm.dbtools.schemacopy.SchemaCopyException;
import at.bxm.dbtools.schemacopy.sequence.SequenceAdjuster;
import at.bxm.dbtools.schemacopy.table.CopyTargetMode;
import at.bxm.dbtools.schemacopy.table.TableCopier;

public class SchemaCopyRunner {

	private static final Logger logger = LoggerFactory.getLogger(SchemaCopyRunner.class);
	private Database source;
	private Database target;
	private int batchsize = 100;
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

	public void setBatchsize(int value) {
		batchsize = value;
	}

	public void copy(CopyTargetMode mode) {
		TableCopier tc = new TableCopier(source, target, batchsize);
		SequenceAdjuster sa = new SequenceAdjuster(source, target);
		String line;
		BufferedReader in = null;
		long startTime = System.currentTimeMillis();
		int count = 0;
		try {
			in = new BufferedReader(getCsvDataReader());
			while ((line = in.readLine()) != null) {
				if (!line.startsWith("#")) {
					String[] tokens = line.split(";");
					String tablename = tokens[0];
					String query = tokens.length > 1 ? StringUtils.trimToNull(tokens[1]) : null;
					if (query != null) {
						tc.copyFromQuery(query, tablename, target.getSchemaName(), mode);
					} else {
						tc.copy(tablename, source.getSchemaName(), tablename, target.getSchemaName(), mode);
					}
					for (int i = 2; i < tokens.length; i++) {
						if (StringUtils.isNotBlank(tokens[i])) {
							sa.adjust(tokens[i], source.getSchemaName(), target.getSchemaName());
						}
					}
					count++;
				}
			}
		} catch (IOException e) {
			throw new SchemaCopyException("Could not read CSV data", e);
		} finally {
			logger.info(count + " tables processed, " + (System.currentTimeMillis() - startTime) + " ms");
			if (in != null) {
				try {
					in.close();
				} catch (IOException ignore) {
				}
			}
		}
	}

	public static void main(String[] args) throws IOException {
		String propertiesfile = args != null && args.length > 0 ? args[0] : null;
		Reader is = null;
		try {
			Properties p;
			if (propertiesfile != null) {
				p = new Properties();
				is = open(propertiesfile);
				p.load(is);
			} else {
				p = System.getProperties();
			}
			SchemaCopyRunner scr = new SchemaCopyRunner();
			scr.setSource(new Database(
				new DriverManagerDataSource(p.getProperty("schemacopy.source.url"),
					p.getProperty("schemacopy.source.username"), p.getProperty("schemacopy.source.password")),
				Dialect.valueOf(p.getProperty("schemacopy.source.dialect").toUpperCase()),
				p.getProperty("schemacopy.source.schemaname")));
			scr.setTarget(new Database(
				new DriverManagerDataSource(p.getProperty("schemacopy.target.url"),
					p.getProperty("schemacopy.target.username"), p.getProperty("schemacopy.target.password")),
				Dialect.valueOf(p.getProperty("schemacopy.target.dialect").toUpperCase()),
				p.getProperty("schemacopy.target.schemaname")));
			scr.setCsvDataResource(p.getProperty("schemacopy.datafile"));
			String batchsize = StringUtils.trimToNull(p.getProperty("schemacopy.batchsize"));
			if (batchsize != null) {
				scr.setBatchsize(Integer.parseInt(batchsize));
			}
			scr.copy(CopyTargetMode.valueOf(p.getProperty("schemacopy.copymode").toUpperCase()));
		} finally {
			if (is != null) {
				is.close();
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
				throw new FileNotFoundException(file.getAbsolutePath());
			}
		}
	}

}
