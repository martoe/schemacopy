package at.bxm.dbtools.schemacopy;

import org.springframework.jdbc.core.RowCallbackHandler;

public interface TableCopyTarget extends RowCallbackHandler {

	int getRowsProcessed();

}
