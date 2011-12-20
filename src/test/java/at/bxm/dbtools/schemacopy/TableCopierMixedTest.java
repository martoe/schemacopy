package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

/** Test the {@link TableCopier} class between H2 and Oracle databases */
public class TableCopierMixedTest {

	private JdbcTemplate jtSource;
	private JdbcTemplate jtTarget;

	//	create user targettest identified by test;
	@Test
	public void copyLobFromOracleToH2() {
		// GIVEN: a non-empty Oracle table with LOBs and an empty H2 table
		final int datasets = 123;
		jtSource = Oracle.createLobTableWithData(Oracle.USERNAME_SOURCE, 123);
		jtTarget = H2.createLobTable("target");

		// WHEN: copying
		TableCopier tc = new TableCopier();
		tc.setSource(jtSource.getDataSource());
		tc.setTarget(jtTarget.getDataSource());
		tc.copy(Oracle.LOBTABLE_NAME, null, null, null, "c_id");

		// THEN: target table contains equal dataset count
		assertEquals(datasets, jtTarget.queryForLong(H2.LOBTABLE_COUNTQUERY));
	}

	@Test
	public void copyLobFromH2ToOracle() {
		// GIVEN: a non-empty H2 table with LOBs and an empty Oracle table
		final int datasets = 123;
		jtSource = H2.createLobTableWithData("source", 123);
		jtTarget = Oracle.createLobTable(Oracle.USERNAME_TARGET);

		// WHEN: copying
		TableCopier tc = new TableCopier();
		tc.setSource(jtSource.getDataSource());
		tc.setTarget(jtTarget.getDataSource());
		tc.copy(Oracle.LOBTABLE_NAME, null, null, null, "c_id");

		// THEN: target table contains equal dataset count
		assertEquals(datasets, jtTarget.queryForLong(Oracle.LOBTABLE_COUNTQUERY));
	}

}
