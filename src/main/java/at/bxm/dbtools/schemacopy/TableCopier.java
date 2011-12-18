package at.bxm.dbtools.schemacopy;


// TODO test with oracle
public class TableCopier extends BaseCopier {

	/**
	 * @param sourceTableName (required)
	 * @param sourceSchemaName (optional)
	 * @param targetTableName (optional)
	 * @param targetSchemaName (optional)
	 * @param sortColumn (optional)
	 * @return the number of datasets that have been copied
	 */
	public int copy(String sourceTableName, String sourceSchemaName, String targetTableName, String targetSchemaName,
		String sortColumn) {
		final TableCopyTarget td = new DatabaseTableCopyTarget(target, targetTableName, targetSchemaName, 100);
		final long time = System.currentTimeMillis();
		final String qualifiedTableName = sourceSchemaName != null ? sourceSchemaName + "." + sourceTableName
			: sourceTableName;
		final String query = "select * from " + qualifiedTableName + (sortColumn != null ? " order by " + sortColumn : "");
		sqlLogger.debug(query);
		source.query(query, td);
		logger.info("Table " + qualifiedTableName + " copied: " + td.getRowsProcessed() + " datasets, "
			+ (System.currentTimeMillis() - time) + " ms");
		return td.getRowsProcessed();
	}

}
