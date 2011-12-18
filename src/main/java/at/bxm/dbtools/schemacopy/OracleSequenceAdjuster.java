package at.bxm.dbtools.schemacopy;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * No clean way to get the current sequence value:
 * (1) "select myseq.currval from dual" only works with a preceeding "select myseq.nextval from dual" (same session)
 * (2) "select last_number from user_sequences" includes cached values
 * (3) "select myseq.nextval from dual" changes the sequence
 */
public class OracleSequenceAdjuster extends BaseCopier implements SequenceAdjuster {

	static final String INCREMENT_QUERY = "select increment_by from user_sequences where upper(sequence_name)=upper(?)";

	@Override
	public void adjust(String sequenceName, String sourceSchemaName, String targetSchemaName) {
		final String sourceName = sourceSchemaName != null ? sourceSchemaName + "." + sequenceName : sequenceName;
		final String targetName = targetSchemaName != null ? targetSchemaName + "." + sequenceName : sequenceName;
		final long currentSourceValue = nextSequenceValue(source, sourceName);
		final long increment = source.queryForLong(INCREMENT_QUERY, sourceName);
		// we cannot reset the sequence directly: we must consume the delta value first...
		final long currentTargetValue = nextSequenceValue(target, sourceName);
		final long delta = currentSourceValue - currentTargetValue;
		if (delta != 0) {
			target.execute("alter sequence " + targetName + " increment by "
				+ delta + " minvalue " + Math.min(delta, 0));
			target.queryForLong("select " + targetName + ".nextval from dual");
		}
		// ...and then set the increment value:
		target.execute("alter sequence " + targetName + " increment by " + increment);
	}

	private long nextSequenceValue(JdbcTemplate database, String sequenceName) {
		return database.queryForLong("select " + sequenceName + ".nextval from dual");
	}

}
