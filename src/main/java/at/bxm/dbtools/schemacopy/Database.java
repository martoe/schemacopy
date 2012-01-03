package at.bxm.dbtools.schemacopy;

import at.bxm.dbtools.schemacopy.sequence.H2SequenceStrategy;
import at.bxm.dbtools.schemacopy.sequence.OracleSequenceStrategy;
import at.bxm.dbtools.schemacopy.sequence.SequenceStrategy;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/** The source or target of a cloning operation */
public class Database /*extends JdbcTemplate*/{
	private static final String LOCAL_PASSWORD = "$0mePa55w0rd";
	private static final String LOCAL_USERNAME = "schemacopy";
	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final DataSource dataSource;
	private final Dialect dialect;
	private final String schemaName;
	private final JdbcTemplate template;

	/** using a "real" database */
	public Database(DataSource dataSource, Dialect dialect, String schemaName) {
		this(null, dataSource, dialect, schemaName);
	}

	/** using a file-based database (for importing/exporting) */
	public Database(File localFile, String schemaName) { // TODO only called from tests
		this(localFile, new DriverManagerDataSource("jdbc:h2:file:" + localFile.getAbsolutePath(),
			LOCAL_USERNAME, LOCAL_PASSWORD), Dialect.H2, schemaName);
	}

	private Database(File localFile, DataSource dataSource, Dialect dialect, String schemaName) {
		if (dataSource == null || dialect == null) {
			throw new IllegalArgumentException("Parameter missing");
		}
		if (localFile != null) {
			logger.info("Database based on local file: " + localFile.getAbsolutePath());
		}
		this.dataSource = dataSource;
		this.dialect = dialect;
		this.schemaName = StringUtils.trimToNull(schemaName);
		template = new JdbcTemplate(dataSource);
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

	public SequenceStrategy getSequenceStrategy() {
		switch (dialect) {
			case ORACLE:
				return new OracleSequenceStrategy(template);
			case H2:
				return new H2SequenceStrategy(template);
			default:
				throw new IllegalArgumentException("No strategy for " + dialect);
		}
	}

	public JdbcTemplate getTemplate() {
		return template;
	}

	// TODO only called from tests
	void execute(String sql) {
		template.execute(sql);
	}

	// TODO only called from tests
	public long queryForLong(String sql) {
		return template.queryForLong(sql);
	}

	public void query(final String sqlStatement, RowCallbackHandler rch) throws DataAccessException {
		template.query(new PreparedStatementCreator() {
			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
				return con.prepareStatement(sqlStatement, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			}
		}, rch);
	}

}
