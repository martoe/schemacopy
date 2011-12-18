package at.bxm.dbtools.schemacopy;

public class SequenceAdjuster extends BaseCopier {

	public void adjust(String sequenceName, String sourceSchemaName, String targetSchemaName) {
		//		final String qualifiedName = sourceSchemaName != null ? sourceSchemaName + "." + sourceTableName
		//			: sourceTableName;
		//		int start = source.queryForInt("select next value for " + qualifiedName);
		//		
		//		"alter sequence X restart with Y increment by Z" 
		//		final TableCopyTarget td = new DatabaseTableCopyTarget(target, targetTableName, targetSchemaName, 100);
		//			final long time = System.currentTimeMillis();
		//			final String query = "select * from " + qualifiedTableName + (sortColumn != null ? " order by " + sortColumn : "");
		//			sqlLogger.debug(query);
		//			source.query(query, td);
		//			logger.info("Table " + qualifiedTableName + " copied: " + td.getRowsProcessed() + " datasets, "
		//				+ (System.currentTimeMillis() - time) + " ms");
		//			return td.getRowsProcessed();
		//		}
		// TODO implement
	}

}
