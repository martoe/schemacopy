package at.bxm.dbtools.schemacopy.sequence;


import org.springframework.jdbc.core.JdbcTemplate;

/**
 * No clean way to get the current sequence value:
 * (1) "select myseq.currval from dual" only works with a preceeding "select myseq.nextval from dual" (same session)
 * (2) "select last_number from user_sequences" includes cached values
 * (3) "select myseq.nextval from dual" changes the sequence
 */
public class OracleSequenceStrategy implements SequenceStrategy {

	static final String INCREMENT_QUERY = "select increment_by from user_sequences where upper(sequence_name)=upper(?)";
	private final JdbcTemplate database;

	public OracleSequenceStrategy(JdbcTemplate database) {
		this.database = database;
	}

	@Override
	public long sequenceValue(String sequenceName) {
		return database.queryForLong("select " + sequenceName + ".nextval from dual");
	}

	@Override
	public long incrementValue(String sequenceName) {
		return database.queryForLong(INCREMENT_QUERY, sequenceName);
	}

	@Override
	public void updateSequence(String sequenceName, long sequenceValue, long increment) {
		// we cannot reset the sequence directly: we must consume the delta value first...
		final long currentValue = sequenceValue(sequenceName);
		final long delta = sequenceValue - currentValue;
		if (delta != 0) {
			database.execute("alter sequence " + sequenceName + " increment by "
				+ delta + " minvalue " + Math.min(delta, 0));
			database.queryForLong("select " + sequenceName + ".nextval from dual");
		}
		// ...and then set the increment value:
		database.execute("alter sequence " + sequenceName + " increment by " + increment);
	}

}
