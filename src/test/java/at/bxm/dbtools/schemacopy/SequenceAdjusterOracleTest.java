package at.bxm.dbtools.schemacopy;

import static at.bxm.dbtools.schemacopy.Oracle.*;
import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Test;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

public class SequenceAdjusterOracleTest {

	private static final String SEQ_NAME = "seq_test";
	private static final String SEQ_NEXTVALUE = "select " + SEQ_NAME + ".nextval from dual";
	private JdbcTemplate jtSource;
	private JdbcTemplate jtTarget;

	@Test
	public void adjust_new() {
		// GIVEN: two new sequences
		jtSource = createSequence(USERNAME_SOURCE, 10, 100);
		jtTarget = createSequence(USERNAME_TARGET, 1, 3);

		jtSource.query("select * from user_sequences", new RowCallbackHandler() {
			@Override
			public void processRow(ResultSet rs) throws SQLException {
				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
					System.out.println(rs.getMetaData().getColumnName(i) + ": " + rs.getObject(i).getClass().getName() + " = "
						+ rs.getObject(i));
				}
			}
		});

		// WHEN: adjusting
		OracleSequenceAdjuster sa = new OracleSequenceAdjuster();
		sa.setSource(jtSource.getDataSource());
		sa.setTarget(jtTarget.getDataSource());
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
		OracleSequenceAdjuster sa = new OracleSequenceAdjuster();
		sa.setSource(jtSource.getDataSource());
		sa.setTarget(jtTarget.getDataSource());
		sa.adjust(SEQ_NAME, null, null);

		// THEN: both sequences are in sync
		assertEquals(jtSource.queryForLong(SEQ_NEXTVALUE), jtTarget.queryForLong(SEQ_NEXTVALUE));
		assertEquals(jtSource.queryForLong(SEQ_NEXTVALUE), jtTarget.queryForLong(SEQ_NEXTVALUE));
	}

	private JdbcTemplate createSequence(String username, int start, int increment) {
		JdbcTemplate database = connect(username);
		try {
			database.execute("drop sequence " + SEQ_NAME);
		} catch (BadSqlGrammarException ignore) {
		}
		database.execute("create sequence " + SEQ_NAME + " start with " + start + " increment by " + increment);
		//database.execute("select next value for " + SEQ_NAME); // move the sequence to the start value
		//		assertEquals(start, database.queryForLong(SequenceAdjuster.CURRVALUE_QUERY, SEQ_NAME));
		//		assertEquals(increment, database.queryForLong(SequenceAdjuster.INCREMENT_QUERY, SEQ_NAME));
		return database;
	}

}
