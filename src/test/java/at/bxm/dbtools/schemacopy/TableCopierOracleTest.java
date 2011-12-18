package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.junit.Test;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;

public class TableCopierOracleTest extends OracleTestBase {

	private static final String TABLENAME = "lobtable";
	private static final String LOBCOUNT_QUERY = "select count(1) from " + TABLENAME;

	//	create user targettest identified by test;
	@Test
	public void copyLob() {
		// GIVEN: a non-empty source table with LOBs
		jtSource = createLobTable(USERNAME_SOURCE);
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
		jtTarget = createLobTable(USERNAME_TARGET);
		assertEquals(0, jtTarget.queryForLong(LOBCOUNT_QUERY));

		// WHEN: copying
		TableCopier tc = new TableCopier();
		tc.setSource(jtSource.getDataSource());
		tc.setTarget(jtTarget.getDataSource());
		tc.copy(TABLENAME, null, null, null, "c_id");

		// THEN: target table contains equal dataset count
		assertEquals(datasets, jtTarget.queryForLong(LOBCOUNT_QUERY));
	}

	private JdbcTemplate createLobTable(String username) {
		JdbcTemplate database = connect(username);
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

}
