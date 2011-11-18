package at.bxm.dbtools.schemacopy;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

// TODO handle LOBs
public class TableCopier {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final Logger sqlLogger = LoggerFactory.getLogger(getClass().getPackage().getName() + ".SQL");
	private JdbcTemplate source;
	private JdbcTemplate target;

	public void setSource(DataSource value) {
		source = new JdbcTemplate(value);
	}

	public void setTarget(DataSource value) {
		target = new JdbcTemplate(value);
	}

	public int copy(final String tableName, final String sortColumn) {
		final TableData td = new TableData(tableName, 100);
		final long time = System.currentTimeMillis();
		final String query = "select * from " + tableName + (sortColumn != null ? " order by " + sortColumn : "");
		sqlLogger.debug(query);
		source.query(query, td);
		td.flush();
		logger.info("Table " + tableName + " copied: " + td.rowsProcessed + " datasets, "
				+ (System.currentTimeMillis() - time) + " ms");
		return td.rowsProcessed;
	}

	private class TableData implements RowCallbackHandler {
		private final String tableName;
		private final int batchSize;
		private final List<Object[]> cache;
		private String insertSql;
		private int columnCount;
		private int rowsProcessed = 0;

		public TableData(final String tableName, final int batchSize) {
			this.tableName = tableName;
			this.batchSize = batchSize;
			cache = new ArrayList<Object[]>(batchSize);
		}

		private void setMetadata(ResultSetMetaData metadata) throws SQLException {
			columnCount = metadata.getColumnCount();
			StringBuilder sb = new StringBuilder("insert into " + metadata.getTableName(1) + "(");
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
			if (cache.size() >= batchSize) {
				flush();
			}
		}

		public void flush() {
			final int count = cache.size();
			if (count > 0) {
				logger.debug("Writing " + count + " datasets to " + tableName);
				sqlLogger.debug(insertSql);
				target.batchUpdate(insertSql, new BatchPreparedStatementSetter() {

					public void setValues(PreparedStatement ps, int index) throws SQLException {
						Object[] row = cache.get(index);
						for (int i = 0; i < columnCount; i++) {
							ps.setObject(i + 1, row[i]);
						}
					}

					public int getBatchSize() {
						return count;
					}
				});
				rowsProcessed += count;
				cache.clear();
			}
		}
	}

}
