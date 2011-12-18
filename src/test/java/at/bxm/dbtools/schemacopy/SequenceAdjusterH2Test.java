package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;

public class SequenceAdjusterH2Test extends H2TestBase {

	private static final String SEQ_NAME = "seq_test";
	private static final String SEQ_NEXTVALUE = "select next value for " + SEQ_NAME;

	@Test
	public void adjust() {
		// GIVEN: two sequences
		jtSource = createSequence("source", 10, 100);
		jtTarget = createSequence("target", 1, 2);

		//		jtSource.query("select * from information_schema.sequences ", new RowCallbackHandler() {
		//			@Override
		//			public void processRow(ResultSet rs) throws SQLException {
		//				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
		//					System.out.println(rs.getMetaData().getColumnName(i) + ": " + rs.getObject(i).getClass().getName() + " = " + rs.getObject(i));
		//				}
		//			}
		//		});

		// WHEN: adjusting
		H2SequenceAdjuster sa = new H2SequenceAdjuster();
		sa.setSource(jtSource.getDataSource());
		sa.setTarget(jtTarget.getDataSource());
		sa.adjust(SEQ_NAME, null, null);

		// THEN: both sequences are in sync
		assertEquals(jtSource.queryForLong(SEQ_NEXTVALUE), jtTarget.queryForLong(SEQ_NEXTVALUE));
		assertEquals(jtSource.queryForLong(SEQ_NEXTVALUE), jtTarget.queryForLong(SEQ_NEXTVALUE));
	}

	@Test(expected = SchemaCopyException.class)
	public void adjust_invalidSequenceName() {
		// GIVEN: two sequences
		jtSource = createSequence("source", 10, 100);
		jtTarget = createSequence("target", 1, 2);

		// WHEN: adjusting with wrong name
		H2SequenceAdjuster sa = new H2SequenceAdjuster();
		sa.setSource(jtSource.getDataSource());
		sa.setTarget(jtTarget.getDataSource());
		sa.adjust("test", null, null);

		// THEN: exception
	}

	private JdbcTemplate createSequence(String databaseName, int start, int increment) {
		JdbcTemplate database = createDatabase(databaseName);
		database.execute("create sequence " + SEQ_NAME + " start with " + start + " increment by " + increment);
		assertEquals(start, database.queryForLong(SEQ_NEXTVALUE)); // move the sequence to the start value
		return database;
	}

}
