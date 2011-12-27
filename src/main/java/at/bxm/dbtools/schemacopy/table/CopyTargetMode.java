package at.bxm.dbtools.schemacopy.table;

/**
 * How to handle the to-be-copied database objects at the target database
 *  
 * TODO implement CREATE_IF_MISSING, DELETE
 */
public enum CopyTargetMode {

	/** Reuse existing datastructures, fail if the target datastructure doesn't exist */
	REUSE,
	/** Create the target datastructurem fail if it already exists */
	CREATE;

}
