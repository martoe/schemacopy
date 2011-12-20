package at.bxm.dbtools.schemacopy;

import static at.bxm.dbtools.schemacopy.Oracle.*;
import static org.junit.Assert.*;

import org.junit.Test;

public class SequenceAdjusterOracleTest extends TestBase {

	private Database jtSource;
	private Database jtTarget;

	@Test
	public void adjust_new() {
		// GIVEN: two new sequences
		jtSource = createSequence(USERNAME_SOURCE, 10, 100);
		jtTarget = createSequence(USERNAME_TARGET, 1, 3);

		//		jtSource.query("select * from user_sequences", new RowCallbackHandler() {
		//			@Override
		//			public void processRow(ResultSet rs) throws SQLException {
		//				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
		//					System.out.println(rs.getMetaData().getColumnName(i) + ": " + rs.getObject(i).getClass().getName() + " = " + rs.getObject(i));
		//				}
		//			}
		//		});

		// WHEN: adjusting
		SequenceAdjuster sa = new SequenceAdjuster();
		sa.setSource(jtSource);
		sa.setTarget(jtTarget);
		sa.adjust(SEQ_NAME, null, null);

		// THEN: both sequences are in sync
		assertEquals(jtSource.queryForLong(SEQ_NEXTVALUE), jtTarget.queryForLong(SEQ_NEXTVALUE));
		assertEquals(jtSource.queryForLong(SEQ_NEXTVALUE), jtTarget.queryForLong(SEQ_NEXTVALUE));
	}

	@Test
	public void adjust_used() {
		// GIVEN: two used sequences
		jtSource = createSequence(USERNAME_SOURCE, 3, 100);
		jtSource.queryForLong(SEQ_NEXTVALUE);
		jtTarget = createSequence(USERNAME_TARGET, 21, 5);
		jtTarget.queryForLong(SEQ_NEXTVALUE);

		// WHEN: adjusting
		SequenceAdjuster sa = new SequenceAdjuster();
		sa.setSource(jtSource);
		sa.setTarget(jtTarget);
		sa.adjust(SEQ_NAME, null, null);

		// THEN: both sequences are in sync
		assertEquals(jtSource.queryForLong(SEQ_NEXTVALUE), jtTarget.queryForLong(SEQ_NEXTVALUE));
		assertEquals(jtSource.queryForLong(SEQ_NEXTVALUE), jtTarget.queryForLong(SEQ_NEXTVALUE));
	}

}
