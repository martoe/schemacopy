package at.bxm.dbtools.schemacopy;

import static at.bxm.dbtools.schemacopy.H2.*;
import static org.junit.Assert.*;

import org.junit.Test;

/** Test the {@link TableCopier} class for a H2 in-memory database */
public class TableCopierH2Test extends H2TestBase {

	@Test
	public void copy() {
		// GIVEN: a non-empty source table
		final int datasets = 1111;
		jtSource = createSimpleTableWithData("source", datasets);
		// AND: an empty target table
		jtTarget = createSimpleTable("target");

		// WHEN: copying
		TableCopier tc = new TableCopier();
		tc.setSource(jtSource.getDataSource());
		tc.setTarget(jtTarget.getDataSource());
		tc.copy("testtable", null, null, null, "c_id");

		// THEN: target table contains equal dataset count
		assertEquals(datasets, jtTarget.queryForLong(COUNT_QUERY));
	}

	@Test
	public void copyLob() {
		// GIVEN: a non-empty source table with LOBs
		final int datasets = 1111;
		jtSource = createLobTableWithData("source", datasets);
		// AND: an empty target table
		jtTarget = createLobTable("target");
		assertEquals(0, jtTarget.queryForLong(LOBTABLE_COUNTQUERY));

		// WHEN: copying
		TableCopier tc = new TableCopier();
		tc.setSource(jtSource.getDataSource());
		tc.setTarget(jtTarget.getDataSource());
		tc.copy("lobtable", null, null, null, "c_id");

		// THEN: target table contains equal dataset count
		assertEquals(datasets, jtTarget.queryForLong(LOBTABLE_COUNTQUERY));
	}

}
