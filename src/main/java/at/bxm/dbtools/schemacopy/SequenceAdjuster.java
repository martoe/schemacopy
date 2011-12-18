package at.bxm.dbtools.schemacopy;

import org.springframework.dao.EmptyResultDataAccessException;

public class SequenceAdjuster extends BaseCopier {

	static final String CURRVALUE_QUERY = "select current_value from information_schema.sequences where upper(sequence_name)=upper(?)";
	static final String INCREMENT_QUERY = "select increment from information_schema.sequences where upper(sequence_name)=upper(?)";

	public void adjust(String sequenceName, String sourceSchemaName, String targetSchemaName) {
		final String sourceName = sourceSchemaName != null ? sourceSchemaName + "." + sequenceName : sequenceName;
		try {
			final long currentValue = source.queryForLong(CURRVALUE_QUERY, sourceName);
			final long increment = source.queryForLong(INCREMENT_QUERY, sourceName);
			final long nextValue = currentValue + increment;
			final String targetName = targetSchemaName != null ? targetSchemaName + "." + sequenceName : sequenceName;
			target.execute("alter sequence " + targetName + " restart with " + nextValue + " increment by " + increment);
		} catch (EmptyResultDataAccessException e) {
			throw new SchemaCopyException("Sequence \"" + sourceName + "\" doesn't exist in source database");
		}
	}

}
