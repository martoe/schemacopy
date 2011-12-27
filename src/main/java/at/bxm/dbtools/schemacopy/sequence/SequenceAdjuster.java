package at.bxm.dbtools.schemacopy.sequence;

import at.bxm.dbtools.schemacopy.BaseCopier;
import at.bxm.dbtools.schemacopy.Database;
import at.bxm.dbtools.schemacopy.SchemaCopyException;

import org.springframework.dao.EmptyResultDataAccessException;

public class SequenceAdjuster extends BaseCopier {

	public SequenceAdjuster(Database source, Database target) {
		super(source, target);
	}

	public final void adjust(String sequenceName, String sourceSchemaName, String targetSchemaName) {
		final String sourceName = sourceSchemaName != null ? sourceSchemaName + "." + sequenceName : sequenceName;
		final String targetName = targetSchemaName != null ? targetSchemaName + "." + sequenceName : sequenceName;
		try {
			final long sequenceValue = source.getSequenceStrategy().sequenceValue(sourceName);
			final long increment = source.getSequenceStrategy().incrementValue(sourceName);
			target.getSequenceStrategy().updateSequence(targetName, sequenceValue, increment);
		} catch (EmptyResultDataAccessException e) {
			throw new SchemaCopyException("Sequence \"" + sourceName + "\" doesn't exist in source database");
		}
	}

}
