package at.bxm.dbtools.schemacopy;

import org.junit.After;

public class TestBase {

	protected Database sourceDb;
	protected Database targetDb;

	/** Make sure the next test uses an empty H2 database */
	@After
	public void cleanup() {
		if (sourceDb != null && sourceDb.getDialect() == Dialect.H2) {
			sourceDb.execute("drop all objects");
		}
		if (targetDb != null && sourceDb.getDialect() == Dialect.H2) {
			targetDb.execute("drop all objects");
		}
	}

}
