package at.bxm.dbtools.schemacopy;

import static at.bxm.dbtools.schemacopy.H2.*;
import static org.junit.Assert.*;

import org.junit.Test;

/** Test the {@link TableCopier} class for a H2 in-memory database */
public class TableCopierH2Test extends TestBase {

	@Test
	public void copy_reuse() {
		// GIVEN: a non-empty source table and an empty target table
		final int datasets = 1111;
		sourceDb = createTableWithData("source", datasets);
		targetDb = createTable("target");

		// WHEN: copying in "reuse" mode
		TableCopier tc = new TableCopier(sourceDb, targetDb);
		tc.copy("testtable", null, null, null, CopyTargetMode.REUSE);

		// THEN: target table contains equal dataset count
		assertEquals(datasets, targetDb.queryForLong(TABLE_COUNTQUERY));
	}

	@Test
	public void copy_create() {
		// GIVEN: a non-empty source table and an empty target database
		final int datasets = 1111;
		sourceDb = createTableWithData("source", datasets);
		targetDb = createInMemoryDatabase("target");

		// WHEN: copying in "reuse" mode
		TableCopier tc = new TableCopier(sourceDb, targetDb);
		tc.copy("testtable", null, null, null, CopyTargetMode.CREATE);

		// THEN: target table contains equal dataset count
		assertEquals(datasets, targetDb.queryForLong(TABLE_COUNTQUERY));
	}

	@Test(expected = SchemaCopyException.class)
	public void copy_reuse_tablemissing() {
		// GIVEN: a non-empty source table and an empty target database
		sourceDb = createTableWithData("source", 1);
		targetDb = createInMemoryDatabase("target");

		// WHEN: copying in "reuse" mode
		TableCopier tc = new TableCopier(sourceDb, targetDb);
		tc.copy("testtable", null, null, null, CopyTargetMode.REUSE);

		// THEN: exception
	}

	@Test(expected = SchemaCopyException.class)
	public void copy_create_tableexists() {
		// GIVEN: a non-empty source table and an empty target table
		sourceDb = createTableWithData("source", 1);
		targetDb = createTable("target");

		// WHEN: copying in "create" mode
		TableCopier tc = new TableCopier(sourceDb, targetDb);
		tc.copy("testtable", null, null, null, CopyTargetMode.CREATE);

		// THEN: exception
	}

}
