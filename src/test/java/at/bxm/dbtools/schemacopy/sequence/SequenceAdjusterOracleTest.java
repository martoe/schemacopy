package at.bxm.dbtools.schemacopy.sequence;

import static at.bxm.dbtools.schemacopy.Oracle.*;
import static org.junit.Assert.*;

import at.bxm.dbtools.schemacopy.Oracle;
import at.bxm.dbtools.schemacopy.TestBase;

import at.bxm.dbtools.schemacopy.sequence.SequenceAdjuster;

import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Oracle.class)
public class SequenceAdjusterOracleTest extends TestBase {

	@Test
	public void adjust_new() {
		// GIVEN: two new sequences
		sourceDb = createSequence(USERNAME_SOURCE, 10, 100);
		targetDb = createSequence(USERNAME_TARGET, 1, 3);

		//		jtSource.query("select * from user_sequences", new RowCallbackHandler() {
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

	@Test
	public void adjust_used() {
		// GIVEN: two used sequences
		sourceDb = createSequence(USERNAME_SOURCE, 3, 100);
		sourceDb.queryForLong(SEQ_NEXTVALUE);
		targetDb = createSequence(USERNAME_TARGET, 21, 5);
		targetDb.queryForLong(SEQ_NEXTVALUE);

		// WHEN: adjusting
		SequenceAdjuster sa = new SequenceAdjuster(sourceDb, targetDb);
		sa.adjust(SEQ_NAME, null, null);

		// THEN: both sequences are in sync
		assertEquals(sourceDb.queryForLong(SEQ_NEXTVALUE), targetDb.queryForLong(SEQ_NEXTVALUE));
		assertEquals(sourceDb.queryForLong(SEQ_NEXTVALUE), targetDb.queryForLong(SEQ_NEXTVALUE));
	}

}
