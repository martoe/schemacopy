package at.bxm.dbtools.schemacopy.table;

import static org.junit.Assert.*;

import at.bxm.dbtools.schemacopy.H2;
import at.bxm.dbtools.schemacopy.Oracle;
import at.bxm.dbtools.schemacopy.TestBase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Test the {@link TableCopier} class between H2 and Oracle databases */
@Category(Oracle.class)
public class TableCopierMixedTest extends TestBase {

	//	create user targettest identified by test;
	@Test
	public void copyFromOracleToH2_reuse() {
		// GIVEN: a non-empty Oracle table with LOBs and an empty H2 table
		final int datasets = 123;
		sourceDb = Oracle.createTableWithData(Oracle.USERNAME_SOURCE, 123);
		targetDb = H2.createTable("target");

		// WHEN: copying
		TableCopier tc = new TableCopier(sourceDb, targetDb, 100);
		tc.copy(Oracle.TABLE_NAME, null, null, null, CopyTargetMode.REUSE);

		// THEN: target table contains equal dataset count
		assertEquals(datasets, targetDb.queryForLong(H2.TABLE_COUNTQUERY));
	}

	@Test
	public void copyFromOracleToH2_create() {
		// GIVEN: a non-empty Oracle table with LOBs and an empty H2 database
		final int datasets = 123;
		sourceDb = Oracle.createTableWithData(Oracle.USERNAME_SOURCE, 123);
		targetDb = H2.createInMemoryDatabase("target");

		// WHEN: copying
		TableCopier tc = new TableCopier(sourceDb, targetDb, 100);
		tc.copy(Oracle.TABLE_NAME, null, null, null, CopyTargetMode.CREATE);

		// THEN: target table contains equal dataset count
		assertEquals(datasets, targetDb.queryForLong(H2.TABLE_COUNTQUERY));
	}

	@Test
	public void copyFromH2ToOracle_reuse() {
		// GIVEN: a non-empty H2 table with LOBs and an empty Oracle table
		final int datasets = 123;
		sourceDb = H2.createTableWithData("source", 123);
		targetDb = Oracle.createTable(Oracle.USERNAME_TARGET);

		// WHEN: copying
		TableCopier tc = new TableCopier(sourceDb, targetDb, 100);
		tc.copy(Oracle.TABLE_NAME, null, null, null, CopyTargetMode.REUSE);

		// THEN: target table contains equal dataset count
		assertEquals(datasets, targetDb.queryForLong(Oracle.TABLE_COUNTQUERY));
	}

	@Test
	public void copyFromH2ToOracle_create() {
		// GIVEN: a non-empty H2 table with LOBs and an empty Oracle database
		final int datasets = 123;
		sourceDb = H2.createTableWithData("source", 123);
		targetDb = Oracle.connect(Oracle.USERNAME_TARGET);

		// WHEN: copying
		TableCopier tc = new TableCopier(sourceDb, targetDb, 100);
		tc.copy(Oracle.TABLE_NAME, null, null, null, CopyTargetMode.CREATE);

		// THEN: target table contains equal dataset count
		assertEquals(datasets, targetDb.queryForLong(Oracle.TABLE_COUNTQUERY));
	}

}
