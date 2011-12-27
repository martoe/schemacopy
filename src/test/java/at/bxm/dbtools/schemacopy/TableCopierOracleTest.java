package at.bxm.dbtools.schemacopy;

import static at.bxm.dbtools.schemacopy.Oracle.*;
import static org.junit.Assert.*;

import org.junit.Test;

/** Test the {@link TableCopier} class for an Oracle database */
public class TableCopierOracleTest extends TestBase {

	//	create user targettest identified by test;
	@Test
	public void copyLob_create() {
		// GIVEN: a non-empty source table with LOBs and an empty target database
		final int datasets = 111;
		sourceDb = createLobTableWithData(USERNAME_SOURCE, datasets);
		targetDb = connect(USERNAME_TARGET);

		// WHEN: copying
		TableCopier tc = new TableCopier(sourceDb, targetDb);
		tc.copy(LOBTABLE_NAME, null, null, null, CopyTargetMode.CREATE);

		// THEN: target table contains equal dataset count
		assertEquals(datasets, targetDb.queryForLong(LOBTABLE_COUNTQUERY));
	}

	@Test
	public void copyLob_reuse() {
		// GIVEN: a non-empty source table with LOBs and an empty target table
		final int datasets = 111;
		sourceDb = createLobTableWithData(USERNAME_SOURCE, datasets);
		targetDb = createLobTable(USERNAME_TARGET);

		// WHEN: copying
		TableCopier tc = new TableCopier(sourceDb, targetDb);
		tc.copy(LOBTABLE_NAME, null, null, null, CopyTargetMode.REUSE);

		// THEN: target table contains equal dataset count
		assertEquals(datasets, targetDb.queryForLong(LOBTABLE_COUNTQUERY));
	}

}
