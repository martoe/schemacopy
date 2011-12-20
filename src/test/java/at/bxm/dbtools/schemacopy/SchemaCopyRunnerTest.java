package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import org.h2.engine.Constants;
import org.junit.After;
import org.junit.Test;

// TODO copy/export/import sequences
// TODO test for mixed databases
public class SchemaCopyRunnerTest extends TestBase {

	@Test
	public void main() throws IOException {
		// GIVEN: a non-empty source table and an empty target table
		final int datasets = 1111;
		sourceDb = H2.createSimpleTableWithData("source", datasets);
		targetDb = H2.createSimpleTable("target");

		// WHEN: executing the programm
		SchemaCopyRunner.main(null);

		// THEN: target table contains datasets
		assertEquals(datasets, targetDb.queryForLong(H2.SIMPLETABLE_COUNTQUERY));
	}

	@Test
	public void exportAndImport() {
		// GIVEN: a non-empty source table
		final int datasets = 1111;
		sourceDb = H2.createSimpleTableWithData("source", datasets);

		// WHEN: exporting to a new file
		SchemaCopyRunner scr = new SchemaCopyRunner();
		scr.setSource(new Database(sourceDb.getDataSource(), Dialect.H2, null));
		assertFalse(LOCAL_DB_FILE.exists());
		new Database(LOCAL_DB, null).execute("create table testtable(" +
			"c_id number not null, " +
			"c_text varchar(100) not null, " +
			"c_number number not null, " +
			"c_date timestamp not null, " +
			"primary key (c_id))"); // FIXME remove this (create the table automatically)
		scr.setTarget(new Database(LOCAL_DB, null));
		scr.setCsvData("testtable;c_id");
		scr.copy();

		// THEN: the file has been written
		assertTrue(LOCAL_DB_FILE.exists());

		// WHEN: importing this file to a new database
		targetDb = H2.createSimpleTable("target");
		scr.setSource(new Database(LOCAL_DB, null));
		scr.setTarget(new Database(targetDb.getDataSource(), Dialect.H2, null));
		scr.copy();

		// THEN: target table contains datasets
		assertEquals(datasets, targetDb.queryForLong(H2.SIMPLETABLE_COUNTQUERY));
	}

	private static final File LOCAL_DB = new File("test-db");
	private static final File LOCAL_DB_FILE = new File(LOCAL_DB.getAbsolutePath() + Constants.SUFFIX_PAGE_FILE);

	@After
	public void tearDown() {
		if (LOCAL_DB_FILE.exists()) {
			LOCAL_DB_FILE.delete();
		}
	}

}
