package at.bxm.dbtools.schemacopy.table;

import static at.bxm.dbtools.schemacopy.ArgumentChecker.*;

import at.bxm.dbtools.schemacopy.BaseCopier;
import at.bxm.dbtools.schemacopy.Database;

public class TableCopier extends BaseCopier {

	private final int batchsize;

	public TableCopier(Database source, Database target, int batchsize) {
		super(source, target);
		checkGreaterThan(batchsize, 0, "batchsize");
		this.batchsize = batchsize;
	}

	/**
	 * @param sourceTableName (required)
	 * @param sourceSchemaName (optional, no qualified access if missing)
	 * @param targetTableName (optional, defaults to "sourceTableName")
	 * @param targetSchemaName (optional)
	 * @param mode (required) 
	 * @return the number of datasets that have been inserted
	 */
	public int copy(String sourceTableName, String sourceSchemaName, String targetTableName, String targetSchemaName,
		CopyTargetMode mode) {
		final TableCopyTarget td = new DatabaseTableCopyTarget(target.getTemplate(),
			targetTableName != null ? targetTableName : sourceTableName,
			targetSchemaName, mode, batchsize);
		final long time = System.currentTimeMillis();
		final String qualifiedTableName = sourceSchemaName != null ? sourceSchemaName + "." + sourceTableName
			: sourceTableName;
		source.query("select * from " + qualifiedTableName, td);
		logger.info("Table " + qualifiedTableName + " copied: " + td.getRowsProcessed() + " datasets, "
			+ (System.currentTimeMillis() - time) + " ms");
		return td.getRowsProcessed();
	}

	/**
	 * @param sqlQuery (required)
	 * @param targetTableName (required)
	 * @param targetSchemaName (optional)
	 * @param mode (required) 
	 * @return the number of datasets that have been inserted
	 */
	public int copyFromQuery(String sqlQuery, String targetTableName, String targetSchemaName, CopyTargetMode mode) {
		final TableCopyTarget td = new DatabaseTableCopyTarget(target.getTemplate(), targetTableName, targetSchemaName,
			mode, batchsize);
		final long time = System.currentTimeMillis();
		source.query(sqlQuery, td);
		logger.info("Query copied to " + targetTableName + ": " + td.getRowsProcessed() + " datasets, "
			+ (System.currentTimeMillis() - time) + " ms");
		return td.getRowsProcessed();
	}

}
