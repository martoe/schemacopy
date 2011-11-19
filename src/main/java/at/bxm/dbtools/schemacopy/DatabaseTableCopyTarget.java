package at.bxm.dbtools.schemacopy;

import static at.bxm.dbtools.schemacopy.TableCopier.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

class DatabaseTableCopyTarget implements TableCopyTarget {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final JdbcTemplate target;
	private final String tableName;
	private final String schemaName;
	private String qualifiedTableName;
	private final int batchSize;
	private final List<Object[]> cache;
	private String insertSql;
	private int columnCount;
	private int rowsProcessed;
	private final BatchPreparedStatementSetter batchSetter = new BatchPreparedStatementSetter() {

		public void setValues(PreparedStatement ps, int index) throws SQLException {
			Object[] row = cache.get(index);
			for (int i = 0; i < columnCount; i++) {
				ps.setObject(i + 1, row[i]);
			}
		}

		public int getBatchSize() {
			return cache.size();
		}
	};

	/**
	 * @param target (required)
	 * @param tableName (optional; defaults to the source table name)
	 * @param schemaName (optional; defaults to the source schema name)
	 */
	DatabaseTableCopyTarget(JdbcTemplate target, String tableName, String schemaName, int batchSize) {
		this.target = target;
		this.tableName = tableName;
		this.schemaName = schemaName;
		this.batchSize = batchSize;
		cache = new ArrayList<Object[]>(batchSize);
	}

	private void setMetadata(ResultSetMetaData metadata) throws SQLException {
		if (qualifiedTableName == null) {
			qualifiedTableName = (schemaName != null ? schemaName : metadata.getSchemaName(1))
				+ "." + (tableName != null ? tableName : metadata.getTableName(1));
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

	public void processRow(ResultSet rs) throws SQLException {
		if (insertSql == null) {
			setMetadata(rs.getMetaData());
		}
		Object[] data = new Object[columnCount];
		for (int i = 1; i <= columnCount; i++) {
			data[i - 1] = rs.getObject(i);
		}
		cache.add(data);
		if (cache.size() >= batchSize || rs.isLast()) {
			flush();
		}
	}

	private void flush() {
		final int count = cache.size();
		if (count > 0) {
			logger.debug("Writing " + count + " datasets to " + qualifiedTableName);
			sqlLogger.debug(insertSql);
			target.batchUpdate(insertSql, batchSetter);
			rowsProcessed += count;
			cache.clear();
		}
	}

	public int getRowsProcessed() {
		return rowsProcessed;
	}

}