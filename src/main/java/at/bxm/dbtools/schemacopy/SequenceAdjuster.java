package at.bxm.dbtools.schemacopy;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;

public class SequenceAdjuster {

	private JdbcTemplate jdbcTemplate;

	public void setDataSource(DataSource value) {
		jdbcTemplate = new JdbcTemplate(value);
	}

	public void adjust(final String tableName, final String idColumn, final String sequenceName) {
		// TODO implement
	}

}
