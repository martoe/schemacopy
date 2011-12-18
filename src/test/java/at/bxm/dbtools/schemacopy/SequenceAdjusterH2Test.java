package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

public class SequenceAdjusterH2Test extends H2TestBase {

	@Test
	public void copy() {
		// GIVEN: two sequences
		jtSource = createSequence("source", 10, 100);
		jtTarget = createSequence("target", 1, 2);

		// WHEN: adjusting
		SequenceAdjuster sa = new SequenceAdjuster();
		sa.setSource(jtSource.getDataSource());
		sa.setTarget(jtTarget.getDataSource());
		sa.adjust("testtable", null, null);

		// THEN: target seqeuence has correct positin and increment
		fail("not implemented");
	}

	private JdbcTemplate createSequence(String databaseName, int start, int increment) {
		JdbcTemplate database = createDatabase(databaseName);
		database.execute("create sequence seq_test start with " + start + " increment by " + increment);
		return database;
	}

}
