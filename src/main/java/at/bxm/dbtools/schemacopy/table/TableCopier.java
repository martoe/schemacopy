package at.bxm.dbtools.schemacopy.table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.PreparedStatementCreator;

import at.bxm.dbtools.schemacopy.BaseCopier;
import at.bxm.dbtools.schemacopy.Database;

public class TableCopier extends BaseCopier {

	public TableCopier(Database source, Database target) {
		super(source, target);
	}

	/**
	 * @param sourceTableName (required)
	 * @param sourceSchemaName (optional, no qualified access if missing)
	 * @param targetTableName (optional, defaults to "sourceTableName")
	 * @param targetSchemaName (optional)
	 * @param mode (required) 
	 * @return the number of datasets that have been copied
	 */
	public int copy(String sourceTableName, String sourceSchemaName, String targetTableName, String targetSchemaName,
		CopyTargetMode mode) {
		final TableCopyTarget td = new DatabaseTableCopyTarget(target.getTemplate(),
			targetTableName != null ? targetTableName : sourceTableName,
			targetSchemaName, mode, 100);
		final long time = System.currentTimeMillis();
		final String qualifiedTableName = sourceSchemaName != null ? sourceSchemaName + "." + sourceTableName
			: sourceTableName;
		final String query = "select * from " + qualifiedTableName;
		source.getTemplate().query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
				return con.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			}
		}, td);
		logger.info("Table " + qualifiedTableName + " copied: " + td.getRowsProcessed() + " datasets, "
			+ (System.currentTimeMillis() - time) + " ms");
		return td.getRowsProcessed();
	}

}
