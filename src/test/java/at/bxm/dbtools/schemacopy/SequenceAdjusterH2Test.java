package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

public class SequenceAdjusterH2Test extends H2TestBase {

	private static final String SEQ_NAME = "seq_test";

	@Test
	public void copy() {
		// GIVEN: two sequences
		jtSource = createSequence("source", 10, 100);
		jtTarget = createSequence("target", 1, 2);

		//		jtSource.query("select * from information_schema.sequences ", new RowCallbackHandler() {
		//			@Override
		//			public void processRow(ResultSet rs) throws SQLException {
		//				// TODO Auto-generated method stub
		//				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
		//					System.out.println(rs.getMetaData().getColumnName(i) + ": " + rs.getObject(i).getClass().getName() + " = " + rs.getObject(i));
		//				}
		//			}
		//		});

		// WHEN: adjusting
		SequenceAdjuster sa = new SequenceAdjuster();
		sa.setSource(jtSource.getDataSource());
		sa.setTarget(jtTarget.getDataSource());
		sa.adjust(SEQ_NAME, null, null);

		// THEN: target seqeuence has correct position and increment
		assertEquals(10, jtTarget.queryForLong(SequenceAdjuster.CURRVALUE_QUERY, SEQ_NAME));
		assertEquals(100, jtTarget.queryForLong(SequenceAdjuster.INCREMENT_QUERY, SEQ_NAME));
	}

	@Test(expected = SchemaCopyException.class)
	public void copy_invalidSequenceName() {
		// GIVEN: two sequences
		jtSource = createSequence("source", 10, 100);
		jtTarget = createSequence("target", 1, 2);

		// WHEN: adjusting with wrong name
		SequenceAdjuster sa = new SequenceAdjuster();
		sa.setSource(jtSource.getDataSource());
		sa.setTarget(jtTarget.getDataSource());
		sa.adjust("test", null, null);

		// THEN: exception
	}

	private JdbcTemplate createSequence(String databaseName, int start, int increment) {
		JdbcTemplate database = createDatabase(databaseName);
		database.execute("create sequence " + SEQ_NAME + " start with " + start + " increment by " + increment);
		database.execute("select next value for " + SEQ_NAME); // move the sequence to the start value
		assertEquals(start, database.queryForLong(SequenceAdjuster.CURRVALUE_QUERY, SEQ_NAME));
		assertEquals(increment, database.queryForLong(SequenceAdjuster.INCREMENT_QUERY, SEQ_NAME));
		return database;
	}

}
