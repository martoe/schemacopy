package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;

public class TableCopierH2Test extends H2TestBase {

	private static final String LOBCOUNT_QUERY = "select count(1) from lobtable";

	@Test
	public void copy() {
		// GIVEN: a non-empty source table
		final int datasets = 1111;
		jtSource = createSimpleTableWithData("source", datasets);
		// AND: an empty target table
		jtTarget = createSimpleTable("target");

		// WHEN: copying
		TableCopier tc = new TableCopier();
		tc.setSource(jtSource.getDataSource());
		tc.setTarget(jtTarget.getDataSource());
		tc.copy("testtable", null, null, null, "c_id");

		// THEN: target table contains equal dataset count
		assertEquals(datasets, jtTarget.queryForLong(COUNT_QUERY));
	}

	@Test
	public void copyLob() {
		// GIVEN: a non-empty source table with LOBs
		jtSource = createLobTable("source");
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
		jtTarget = createLobTable("target");
		assertEquals(0, jtTarget.queryForLong(LOBCOUNT_QUERY));

		// WHEN: copying
		TableCopier tc = new TableCopier();
		tc.setSource(jtSource.getDataSource());
		tc.setTarget(jtTarget.getDataSource());
		tc.copy("lobtable", null, null, null, "c_id");

		// THEN: target table contains equal dataset count
		assertEquals(datasets, jtTarget.queryForLong(LOBCOUNT_QUERY));
	}

	private JdbcTemplate createLobTable(String databaseName) {
		JdbcTemplate database = createInMemoryDatabase(databaseName);
		database.execute("create table lobtable(" +
			"c_id number not null, " +
			"c_clob clob not null, " +
			"c_blob blob not null, " +
			"primary key (c_id))");
		return database;
	}

}
