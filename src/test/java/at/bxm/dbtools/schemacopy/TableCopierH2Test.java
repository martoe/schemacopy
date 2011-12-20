package at.bxm.dbtools.schemacopy;

import static at.bxm.dbtools.schemacopy.H2.*;
import static org.junit.Assert.*;

import org.junit.Test;

/** Test the {@link TableCopier} class for a H2 in-memory database */
public class TableCopierH2Test extends TestBase {

	@Test
	public void copy() {
		// GIVEN: a non-empty source table
		final int datasets = 1111;
		sourceDb = createSimpleTableWithData("source", datasets);
		// AND: an empty target table
		targetDb = createSimpleTable("target");

		// WHEN: copying
		TableCopier tc = new TableCopier(sourceDb, targetDb);
		tc.copy("testtable", null, null, null);

		// THEN: target table contains equal dataset count
		assertEquals(datasets, targetDb.queryForLong(SIMPLETABLE_COUNTQUERY));
	}

	@Test
	public void copyLob() {
		// GIVEN: a non-empty source table with LOBs
		final int datasets = 1111;
		sourceDb = createLobTableWithData("source", datasets);
		// AND: an empty target table
		targetDb = createLobTable("target");
		assertEquals(0, targetDb.queryForLong(LOBTABLE_COUNTQUERY));

		// WHEN: copying
		TableCopier tc = new TableCopier(sourceDb, targetDb);
		tc.copy("lobtable", null, null, null);

		// THEN: target table contains equal dataset count
		assertEquals(datasets, targetDb.queryForLong(LOBTABLE_COUNTQUERY));
	}

}
