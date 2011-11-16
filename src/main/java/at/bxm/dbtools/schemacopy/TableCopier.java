package at.bxm.dbtools.schemacopy;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

public class TableCopier {

	private JdbcTemplate source;
	private JdbcTemplate target;

	public void setSource(DataSource value) {
		source = new JdbcTemplate(value);
	}

	public void setTarget(DataSource value) {
		target = new JdbcTemplate(value);
	}

	public void copy(final String tableName, final String sortColumn) {
		final TableData td = new TableData(100);
		source.query("select * from " + tableName + sortColumn != null ? " order by " + sortColumn : "", td);
		td.flush();
	}

	private class TableData implements RowCallbackHandler {

		private String insertSql;
		private int columnCount;
		private final int batchSize;
		private final List<Object[]> cache;

		public TableData(int batchSize) {
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
			if (cache.size() > batchSize) {
				flush();
			}
		}

		public void flush() {
			target.batchUpdate(insertSql, new BatchPreparedStatementSetter() {

				public void setValues(PreparedStatement ps, int index) throws SQLException {
					Object[] row = cache.get(index);
					for (int i = 1; i <= columnCount; i++) {
						ps.setObject(i, row[i + 1]);
					}
				}

				public int getBatchSize() {
					return cache.size();
				}
			});
			cache.clear();
		}
	}
}
