package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.Test;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;

// FIXME work in progress
public class TableCopierOracleTest {

	private static final String TABLENAME = "lobtable";
	private static final String LOBCOUNT_QUERY = "select count(1) from " + TABLENAME;
	protected JdbcTemplate jtSource;
	protected JdbcTemplate jtTarget;

	// preparing Oracle XE 11g:
	// connect system/manager
	// alter system set processes=150 scope=spfile;
	// [restart database]
	// create user sourcetest identified by test default tablespace users;
	// grant create session to sourcetest;
	// grant create table to sourcetest;
	// grant unlimited tablespace to sourcetest;
	// create user targettest identified by test default tablespace users;
	// grant create session to targettest;
	// grant create table to targettest;
	// grant unlimited tablespace to targettest;

	//	create user targettest identified by test;
	@Test
	public void copyLob() {
		// GIVEN: a non-empty source table with LOBs
		jtSource = createLobTable("sourcetest", "test");
		final LobCreator lobCreator = new DefaultLobHandler().getLobCreator();
		final int datasets = 1111;
		final PreparedStatementSetter pss = new PreparedStatementSetter() {
			private int count = 0;

			@Override
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
		// AND: an empty target table
		jtTarget = createLobTable("targettest", "test");
		assertEquals(0, jtTarget.queryForLong(LOBCOUNT_QUERY));

		// WHEN: copying
		TableCopier tc = new TableCopier();
		tc.setSource(jtSource.getDataSource());
		tc.setTarget(jtTarget.getDataSource());
		tc.copy(TABLENAME, null, null, null, "c_id");

		// THEN: target table contains equal dataset count
		assertEquals(datasets, jtTarget.queryForLong(LOBCOUNT_QUERY));
	}

	private JdbcTemplate createLobTable(String username, String password) {
		JdbcTemplate database = connect(username, password);
		try {
			database.execute("drop table " + TABLENAME);
		} catch (BadSqlGrammarException ignore) {
		}
		database.execute("create table " + TABLENAME + "(" +
			"c_id number not null, " +
			"c_clob clob not null, " +
			"c_blob blob not null, " +
			"primary key (c_id))");
		return database;
	}

	private JdbcTemplate connect(String username, String password) {
		DataSource datasource = new DriverManagerDataSource("jdbc:oracle:thin:@localhost:1521:XE", username, password);
		return new JdbcTemplate(datasource);
	}

}
