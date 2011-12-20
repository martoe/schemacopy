package at.bxm.dbtools.schemacopy;


public interface SequenceStrategy {

	long sequenceValue(String sequenceName);

	long incrementValue(String sequenceName);

	void updateSequence(String sequenceName, long sequenceValue, long increment);

}
