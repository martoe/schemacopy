package at.bxm.dbtools.schemacopy;

import static at.bxm.dbtools.schemacopy.Oracle.*;
import static org.junit.Assert.*;

import org.junit.Test;

/** Test the {@link TableCopier} class for an Oracle database */
public class TableCopierOracleTest extends TestBase {

	private Database jtSource;
	private Database jtTarget;

	//	create user targettest identified by test;
	@Test
	public void copyLob() {
		// GIVEN: a non-empty source table with LOBs and an empty target table
		final int datasets = 111;
		jtSource = createLobTableWithData(USERNAME_SOURCE, datasets);
		jtTarget = createLobTable(USERNAME_TARGET);

		// WHEN: copying
		TableCopier tc = new TableCopier();
		tc.setSource(jtSource);
		tc.setTarget(jtTarget);
		tc.copy(LOBTABLE_NAME, null, null, null, "c_id");

		// THEN: target table contains equal dataset count
		assertEquals(datasets, jtTarget.queryForLong(LOBTABLE_COUNTQUERY));
	}

}
