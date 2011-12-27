package at.bxm.dbtools.schemacopy.sequence;


public interface SequenceStrategy {

	long sequenceValue(String sequenceName);

	long incrementValue(String sequenceName);

	void updateSequence(String sequenceName, long sequenceValue, long increment);

}
