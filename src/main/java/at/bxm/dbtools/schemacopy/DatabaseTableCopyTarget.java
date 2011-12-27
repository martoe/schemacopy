package at.bxm.dbtools.schemacopy;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

class DatabaseTableCopyTarget implements TableCopyTarget {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final JdbcTemplate target;
	private final String tableName;
	private final String schemaName;
	private final CopyTargetMode mode;
	private final int batchSize;
	private final List<Object[]> cache;
	private String qualifiedTableName;
	private String insertSql;
	private int columnCount;
	private int rowsProcessed;
	private final BatchPreparedStatementSetter batchSetter = new BatchPreparedStatementSetter() {

		@Override
		public void setValues(PreparedStatement ps, int index) throws SQLException {
			Object[] row = cache.get(index);
			for (int i = 0; i < columnCount; i++) {
				if (row[i] instanceof Clob) {
					Clob clobOld = (Clob)row[i];
					Clob clobNew = ps.getConnection().createClob();
					clobNew.setString(1, clobOld.getSubString(1, (int)clobOld.length()));
					ps.setClob(i + 1, clobNew);
				} else if (row[i] instanceof Blob) {
					Blob blobOld = (Blob)row[i];
					Blob blobNew = ps.getConnection().createBlob();
					blobNew.setBytes(1, blobOld.getBytes(1, (int)blobOld.length()));
					ps.setBlob(i + 1, blobNew);
				} else {
					ps.setObject(i + 1, row[i]);
				}
			}
		}

		@Override
		public int getBatchSize() {
			return cache.size();
		}
	};

	/**
	 * @param target (required)
	 * @param tableName (required)
	 * @param schemaName (optional, no qualified access if missing)
	 * @param mode (required) 
	 */
	DatabaseTableCopyTarget(JdbcTemplate target, String tableName, String schemaName, CopyTargetMode mode, int batchSize) {
		this.target = target;
		this.tableName = tableName;
		this.schemaName = schemaName;
		this.mode = mode;
		this.batchSize = batchSize;
		cache = new ArrayList<Object[]>(batchSize);
	}

	@Override
	public void processRow(ResultSet rs) throws SQLException {
		if (insertSql == null) {
			setMetadata(rs);
		}
		Object[] data = new Object[columnCount];
		for (int i = 1; i <= columnCount; i++) {
			data[i - 1] = rs.getObject(i);
			if (data[i - 1].getClass().getName().equals("oracle.sql.TIMESTAMP")) {
				// workaround because Oracle returns the wrong object type
				data[i - 1] = rs.getTimestamp(i);
			}
		}
		cache.add(data);
		if (cache.size() >= batchSize || rs.isLast()) {
			flush();
		}
	}

	private void setMetadata(ResultSet rs) throws SQLException {
		// rs.isLast() needs a scrollable resultset
		if (rs.getType() == ResultSet.TYPE_FORWARD_ONLY) {
			throw new SchemaCopyException("Resultset is not scrollable");
		}
		ResultSetMetaData metadata = rs.getMetaData();
		if (qualifiedTableName == null) {
			// metadata.getSchemaName() and metadata.getTableName() doesn't work with Oracle :(
			//			qualifiedTableName = (schemaName != null ? schemaName : metadata.getSchemaName(1))
			//				+ "." + (tableName != null ? tableName : metadata.getTableName(1));
			qualifiedTableName = schemaName != null ? schemaName + "." + tableName : tableName;

			if (mode == CopyTargetMode.CREATE) {
				createTargetTable(metadata);
			}
		}
		columnCount = metadata.getColumnCount();
		StringBuilder sb = new StringBuilder("insert into " + qualifiedTableName + "(");
		for (int i = 1; i < columnCount; i++) {
			sb.append(metadata.getColumnName(i)).append(',');
		}
		sb.append(metadata.getColumnName(columnCount)).append(") values (")
			.append(StringUtils.repeat("?,", columnCount - 1)).append("?)");
		insertSql = sb.toString();
	}

	private void createTargetTable(ResultSetMetaData md) throws SQLException {
		StringBuilder createSql = new StringBuilder("create table ").append(qualifiedTableName).append('(');
		for (int i = 1; i <= md.getColumnCount(); i++) {
			//			logger.trace(md.getColumnLabel(i) + " " + md.getColumnType(i) + "/" + md.getColumnTypeName(i) + "("
			//				+ md.getColumnDisplaySize(i) + ")(" + md.getPrecision(i) + "," + md.getScale(i) + ")");
			if (i > 1) {
				createSql.append(',');
			}
			createSql.append("\n\t").append(md.getColumnName(i)).append(' ');
			switch (md.getColumnType(i)) {
				case Types.VARCHAR:
					createSql.append(md.getColumnTypeName(i)).append('(').append(md.getPrecision(i)).append(')');
					break;
				case Types.NUMERIC:
					createSql.append(md.getColumnTypeName(i));
					final int precision = md.getPrecision(i);
					final int scale = md.getScale(i);
					if (precision > 0 && scale >= 0) {
						createSql.append('(').append(precision).append(',').append(scale).append(')');
					}
					break;
				case Types.BIGINT:
					createSql.append("long"); // "bigint" is understood by H2, but not Oracle
					break;
				// implement other "Types" when necessary
				default:
					createSql.append(md.getColumnTypeName(i));
			}
		}
		createSql.append(')');
		if (logger.isDebugEnabled()) {
			logger.debug(createSql.toString());
		}
		try {
			target.execute(createSql.toString());
		} catch (BadSqlGrammarException e) {
			throw new SchemaCopyException("Could not create target table", e);
		}
	}

	private void flush() {
		final int count = cache.size();
		if (count > 0) {
			logger.debug("Writing " + count + " datasets to " + qualifiedTableName);
			try {
				target.batchUpdate(insertSql, batchSetter);
			} catch (BadSqlGrammarException e) {
				throw new SchemaCopyException("SQL exception, possible because the target table doesn't exist", e);
			}
			rowsProcessed += count;
			cache.clear();
		}
	}

	@Override
	public int getRowsProcessed() {
		return rowsProcessed;
	}

}
