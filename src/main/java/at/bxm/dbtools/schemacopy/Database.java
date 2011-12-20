package at.bxm.dbtools.schemacopy;

import java.io.File;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/** The source or target of a cloning operation */
public class Database {

	private final File localFile;
	private final DataSource dataSource;
	private final Dialect dialect;
	private final String schemaName;

	/** using a "real" database */
	public Database(DataSource dataSource, Dialect dialect, String schemaName) {
		this(null, dataSource, dialect, schemaName);
	}

	/** using a file-based database (for importing/exporting) */
	public Database(File localFile, String schemaName) {
		this(localFile, new DriverManagerDataSource("jdbc:h2:file:" + localFile.getAbsolutePath(),
			"schemacopy", "$0mePa55w0rd"), Dialect.H2, schemaName);
	}

	private Database(File localFile, DataSource dataSource, Dialect dialect, String schemaName) {
		if (dataSource == null || dialect == null) {
			throw new IllegalArgumentException("Parameter missing");
		}
		this.localFile = localFile;
		this.dataSource = dataSource;
		this.dialect = dialect;
		this.schemaName = StringUtils.trimToNull(schemaName);
	}

	public DataSource getDataSource() {
		return dataSource;
	}

	public Dialect getDialect() {
		return dialect;
	}

	public String getSchemaName() {
		return schemaName;
	}

}
