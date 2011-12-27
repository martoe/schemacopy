package at.bxm.dbtools.schemacopy.table;

import org.springframework.jdbc.core.RowCallbackHandler;

interface TableCopyTarget extends RowCallbackHandler {

	int getRowsProcessed();

}
