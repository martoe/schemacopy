package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import javax.sql.DataSource;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;

public class TableCopierH2Test {

	private static final String COUNT_QUERY = "select count(1) from testtable";
	private static final String LOBCOUNT_QUERY = "select count(1) from lobtable";

	@Test
	public void copy() {
		JdbcTemplate jtSource = createSimpleTable("source");
		final int datasets = 1111;
		final PreparedStatementSetter pss = new PreparedStatementSetter() {
			private int count = 0;

			public void setValues(PreparedStatement ps) throws SQLException {
				ps.setInt(1, ++count);
				ps.setString(2, "some text");
				ps.setDouble(3, (double) count / 3);
				ps.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
			}
		};
		for (int i = 0; i < datasets; i++) {
			jtSource.update("insert into testtable (c_id, c_text, c_number, c_date) values (?, ?, ?, ?)", pss);
		}
		assertEquals(datasets, jtSource.queryForLong(COUNT_QUERY));

		JdbcTemplate jtTarget = createSimpleTable("target");
		assertEquals(0, jtTarget.queryForLong(COUNT_QUERY));

		TableCopier tc = new TableCopier();
		tc.setSource(jtSource.getDataSource());
		tc.setTarget(jtTarget.getDataSource());
		tc.copy("testtable", "c_id");
		assertEquals(datasets, jtTarget.queryForLong(COUNT_QUERY));
	}

	@Test
	public void copyLob() {
		JdbcTemplate jtSource = createLobTable("source");
		final LobCreator lobCreator = new DefaultLobHandler().getLobCreator();
		final int datasets = 1111;
		final PreparedStatementSetter pss = new PreparedStatementSetter() {
			private int count = 0;

			public void setValues(PreparedStatement ps) throws SQLException {
				ps.setInt(1, ++count);
				lobCreator.setClobAsString(ps, 2, "some text");
				lobCreator.setBlobAsBytes(ps, 3, "some bytes".getBytes());
			}
		};
		for (int i = 0; i < datasets; i++) {
			jtSource.update("insert into lobtable (c_id, c_clob, c_blob) values (?, ?, ?)", pss);
		}
		assertEquals(datasets, jtSource.queryForLong(LOBCOUNT_QUERY));

		JdbcTemplate jtTarget = createLobTable("target");
		assertEquals(0, jtTarget.queryForLong(LOBCOUNT_QUERY));

		TableCopier tc = new TableCopier();
		tc.setSource(jtSource.getDataSource());
		tc.setTarget(jtTarget.getDataSource());
		tc.copy("lobtable", "c_id");
		assertEquals(datasets, jtTarget.queryForLong(LOBCOUNT_QUERY));
	}

	private JdbcTemplate createSimpleTable(String databaseName) {
		JdbcTemplate database = createDatabase(databaseName);
		database.execute("create table testtable(" +
			"c_id number not null, " +
			"c_text varchar(100), " +
			"c_number number, " +
			"c_date timestamp, " +
			"primary key (c_id))");
		return database;
	}

	private JdbcTemplate createLobTable(String databaseName) {
		JdbcTemplate database = createDatabase(databaseName);
		database.execute("create table lobtable(" +
			"c_id number not null, " +
			"c_clob clob, " +
			"c_blob blob, " +
			"primary key (c_id))");
		return database;
	}

	private JdbcTemplate createDatabase(String name) {
		DataSource datasource = new DriverManagerDataSource("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1", "sa", "");
		return new JdbcTemplate(datasource);
	}

}
