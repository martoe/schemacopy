package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import org.junit.Test;

public class SequenceAdjusterMixedTest extends TestBase {

	private Database jtSource;
	private Database jtTarget;

	@Test
	public void copySequenceFromOracleToH2() {
		// GIVEN: a used Oracle sequence and a new H2 sequence
		jtSource = Oracle.createSequence(Oracle.USERNAME_SOURCE, 300, 150);
		jtSource.queryForLong(Oracle.SEQ_NEXTVALUE);
		jtTarget = H2.createSequence("target", 0, 5);

		// WHEN: adjusting
		SequenceAdjuster sa = new SequenceAdjuster();
		sa.setSource(jtSource);
		sa.setTarget(jtTarget);
		sa.adjust(H2.SEQ_NAME, null, null);

		// THEN: both sequences are in sync
		assertEquals(jtSource.queryForLong(Oracle.SEQ_NEXTVALUE), jtTarget.queryForLong(H2.SEQ_NEXTVALUE));
		assertEquals(jtSource.queryForLong(Oracle.SEQ_NEXTVALUE), jtTarget.queryForLong(H2.SEQ_NEXTVALUE));
	}

	@Test
	public void copySequenceFromH2ToOracle() {
		// GIVEN: a used H2 sequence and a new Oracle sequence
		jtSource = H2.createSequence("source", 0, 5);
		jtSource.queryForLong(H2.SEQ_NEXTVALUE);
		jtTarget = Oracle.createSequence(Oracle.USERNAME_SOURCE, 300, 150);

		// WHEN: adjusting
		SequenceAdjuster sa = new SequenceAdjuster();
		sa.setSource(jtSource);
		sa.setTarget(jtTarget);
		sa.adjust(Oracle.SEQ_NAME, null, null);

		// THEN: both sequences are in sync
		assertEquals(jtSource.queryForLong(H2.SEQ_NEXTVALUE), jtTarget.queryForLong(Oracle.SEQ_NEXTVALUE));
		assertEquals(jtSource.queryForLong(H2.SEQ_NEXTVALUE), jtTarget.queryForLong(Oracle.SEQ_NEXTVALUE));
	}

}
