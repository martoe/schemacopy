package at.bxm.dbtools.schemacopy.sequence;


import org.springframework.jdbc.core.JdbcTemplate;

public class H2SequenceStrategy implements SequenceStrategy {

	private static final String CURRVALUE_QUERY = "select current_value from information_schema.sequences where upper(sequence_name)=upper(?)";
	private static final String INCREMENT_QUERY = "select increment from information_schema.sequences where upper(sequence_name)=upper(?)";
	private final JdbcTemplate database;

	public H2SequenceStrategy(JdbcTemplate database) {
		this.database = database;
	}

	@Override
	public long sequenceValue(String sequenceName) {
		return database.queryForLong(CURRVALUE_QUERY, sequenceName);
	}

	@Override
	public long incrementValue(String sequenceName) {
		return database.queryForLong(INCREMENT_QUERY, sequenceName);
	}

	@Override
	public void updateSequence(String sequenceName, long sequenceValue, long increment) {
		final long nextValue = sequenceValue + increment;
		database.execute("alter sequence " + sequenceName + " restart with " + nextValue + " increment by " + increment);
	}

}
