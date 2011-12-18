package at.bxm.dbtools.schemacopy;


public interface SequenceAdjuster {

	void adjust(String sequenceName, String sourceSchemaName, String targetSchemaName);

}
