package at.bxm.dbtools.schemacopy;

import static at.bxm.dbtools.schemacopy.H2.*;
import static org.junit.Assert.*;

import org.junit.Test;

public class SequenceAdjusterH2Test extends TestBase {

	@Test
	public void adjust() {
		// GIVEN: two sequences
		sourceDb = createSequence("source", 10, 100);
		targetDb = createSequence("target", 1, 2);

		//		jtSource.query("select * from information_schema.sequences ", new RowCallbackHandler() {
		//			@Override
		//			public void processRow(ResultSet rs) throws SQLException {
		//				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
		//					System.out.println(rs.getMetaData().getColumnName(i) + ": " + rs.getObject(i).getClass().getName() + " = " + rs.getObject(i));
		//				}
		//			}
		//		});

		// WHEN: adjusting
		SequenceAdjuster sa = new SequenceAdjuster(sourceDb, targetDb);
		sa.adjust(SEQ_NAME, null, null);

		// THEN: both sequences are in sync
		assertEquals(sourceDb.queryForLong(SEQ_NEXTVALUE), targetDb.queryForLong(SEQ_NEXTVALUE));
		assertEquals(sourceDb.queryForLong(SEQ_NEXTVALUE), targetDb.queryForLong(SEQ_NEXTVALUE));
	}

	@Test(expected = SchemaCopyException.class)
	public void adjust_invalidSequenceName() {
		// GIVEN: two sequences
		sourceDb = createSequence("source", 10, 100);
		targetDb = createSequence("target", 1, 2);

		// WHEN: adjusting with wrong name
		SequenceAdjuster sa = new SequenceAdjuster(sourceDb, targetDb);
		sa.adjust("test", null, null);

		// THEN: exception
	}

}
