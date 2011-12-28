package at.bxm.dbtools.schemacopy.run;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.h2.engine.Constants;
import org.junit.After;
import org.junit.Test;

import at.bxm.dbtools.schemacopy.Database;
import at.bxm.dbtools.schemacopy.Dialect;
import at.bxm.dbtools.schemacopy.H2;
import at.bxm.dbtools.schemacopy.TestBase;
import at.bxm.dbtools.schemacopy.table.CopyTargetMode;

// TODO copy/export/import sequences
public class SchemaCopyRunnerTest extends TestBase {

	private static final File LOCAL_DB = new File("test-db");
	private static final File LOCAL_DB_FILE = new File(LOCAL_DB.getAbsolutePath() + Constants.SUFFIX_PAGE_FILE);

	@Test
	public void main() throws IOException {
		// GIVEN: a non-empty source table and an empty target table
		final int datasets = 1111;
		sourceDb = H2.createTableWithData("source", datasets);
		targetDb = H2.createTable("target");

		// WHEN: executing the programm
		SchemaCopyRunner.main(new String[] { "schemacopy.properties" });

		// THEN: target table contains datasets
		assertEquals(datasets, targetDb.queryForLong(H2.TABLE_COUNTQUERY));
	}

	@Test
	public void exportAndImport() {
		// GIVEN: a non-empty source table
		final int datasets = 1111;
		sourceDb = H2.createTableWithData("source", datasets);

		// WHEN: exporting to a new file
		SchemaCopyRunner scr = new SchemaCopyRunner();
		scr.setSource(new Database(sourceDb.getDataSource(), Dialect.H2, null));
		assertFalse(LOCAL_DB_FILE.exists());
		scr.setTarget(new Database(LOCAL_DB, null));
		scr.setCsvData("testtable");
		scr.copy(CopyTargetMode.CREATE);

		// THEN: the file has been written
		assertTrue(LOCAL_DB_FILE.exists());

		// WHEN: importing this file to another database
		targetDb = H2.createTable("target");
		scr.setSource(new Database(LOCAL_DB, null));
		scr.setTarget(new Database(targetDb.getDataSource(), Dialect.H2, null));
		scr.copy(CopyTargetMode.REUSE);

		// THEN: target table contains datasets
		assertEquals(datasets, targetDb.queryForLong(H2.TABLE_COUNTQUERY));
	}

	@After
	public void tearDown() {
		if (LOCAL_DB_FILE.exists()) {
			LOCAL_DB_FILE.delete();
		}
	}

}
