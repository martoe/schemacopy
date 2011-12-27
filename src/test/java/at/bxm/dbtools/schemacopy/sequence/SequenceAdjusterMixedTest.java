package at.bxm.dbtools.schemacopy.sequence;

import static org.junit.Assert.*;

import at.bxm.dbtools.schemacopy.H2;
import at.bxm.dbtools.schemacopy.Oracle;
import at.bxm.dbtools.schemacopy.TestBase;

import at.bxm.dbtools.schemacopy.sequence.SequenceAdjuster;

import org.junit.Test;

public class SequenceAdjusterMixedTest extends TestBase {

	@Test
	public void copySequenceFromOracleToH2() {
		// GIVEN: a used Oracle sequence and a new H2 sequence
		sourceDb = Oracle.createSequence(Oracle.USERNAME_SOURCE, 300, 150);
		sourceDb.queryForLong(Oracle.SEQ_NEXTVALUE);
		targetDb = H2.createSequence("target", 0, 5);

		// WHEN: adjusting
		SequenceAdjuster sa = new SequenceAdjuster(sourceDb, targetDb);
		sa.adjust(H2.SEQ_NAME, null, null);

		// THEN: both sequences are in sync
		assertEquals(sourceDb.queryForLong(Oracle.SEQ_NEXTVALUE), targetDb.queryForLong(H2.SEQ_NEXTVALUE));
		assertEquals(sourceDb.queryForLong(Oracle.SEQ_NEXTVALUE), targetDb.queryForLong(H2.SEQ_NEXTVALUE));
	}

	@Test
	public void copySequenceFromH2ToOracle() {
		// GIVEN: a used H2 sequence and a new Oracle sequence
		sourceDb = H2.createSequence("source", 0, 5);
		sourceDb.queryForLong(H2.SEQ_NEXTVALUE);
		targetDb = Oracle.createSequence(Oracle.USERNAME_SOURCE, 300, 150);

		// WHEN: adjusting
		SequenceAdjuster sa = new SequenceAdjuster(sourceDb, targetDb);
		sa.adjust(Oracle.SEQ_NAME, null, null);

		// THEN: both sequences are in sync
		assertEquals(sourceDb.queryForLong(H2.SEQ_NEXTVALUE), targetDb.queryForLong(Oracle.SEQ_NEXTVALUE));
		assertEquals(sourceDb.queryForLong(H2.SEQ_NEXTVALUE), targetDb.queryForLong(Oracle.SEQ_NEXTVALUE));
	}

}
