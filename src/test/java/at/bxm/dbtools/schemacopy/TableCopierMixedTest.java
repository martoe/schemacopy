package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import org.junit.Test;

/** Test the {@link TableCopier} class between H2 and Oracle databases */
public class TableCopierMixedTest extends TestBase {

	//	create user targettest identified by test;
	@Test
	public void copyLobFromOracleToH2() {
		// GIVEN: a non-empty Oracle table with LOBs and an empty H2 table
		final int datasets = 123;
		sourceDb = Oracle.createLobTableWithData(Oracle.USERNAME_SOURCE, 123);
		targetDb = H2.createLobTable("target");

		// WHEN: copying
		TableCopier tc = new TableCopier(sourceDb, targetDb);
		tc.copy(Oracle.LOBTABLE_NAME, null, null, null);

		// THEN: target table contains equal dataset count
		assertEquals(datasets, targetDb.queryForLong(H2.LOBTABLE_COUNTQUERY));
	}

	@Test
	public void copyLobFromH2ToOracle() {
		// GIVEN: a non-empty H2 table with LOBs and an empty Oracle table
		final int datasets = 123;
		sourceDb = H2.createLobTableWithData("source", 123);
		targetDb = Oracle.createLobTable(Oracle.USERNAME_TARGET);

		// WHEN: copying
		TableCopier tc = new TableCopier(sourceDb, targetDb);
		tc.copy(Oracle.LOBTABLE_NAME, null, null, null);

		// THEN: target table contains equal dataset count
		assertEquals(datasets, targetDb.queryForLong(Oracle.LOBTABLE_COUNTQUERY));
	}

}
