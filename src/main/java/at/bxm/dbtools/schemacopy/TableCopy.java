package at.bxm.dbtools.schemacopy;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

public class TableCopy {

	private JdbcTemplate source;
	private JdbcTemplate target;

	public void setSource(DataSource value) {
		source = new JdbcTemplate(value);
	}
	public void setTarget(DataSource value) {
		target = new JdbcTemplate(value);
	}

	public void copy(String tablename, String idColumn) {
		source.query("select * from " + tablename + " order by " + idColumn, new RowCallbackHandler() {
			public void processRow(ResultSet rs) throws SQLException {
				// TODO Auto-generated method stub
				
			}
		});
		

	}
}
