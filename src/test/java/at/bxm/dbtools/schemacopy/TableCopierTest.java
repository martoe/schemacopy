package at.bxm.dbtools.schemacopy;

import java.sql.Connection;
import org.junit.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public class TableCopierTest {

	@Test
	public void copy() {
		DriverManagerDataSource dataSource = new DriverManagerDataSource("jdbc:h2:~/test", "sa", "");
		Connection conn = dataSource.getConnection();
		// RunScript.execute(conn, reader);
		conn.close();
		DriverManagerDataSource dsTarget = new DriverManagerDataSource("jdbc:h2:~/target", "sa", "");
		TableCopier tc = new TableCopier();
		tc.setSource(dataSource);
		tc.setTarget(dsTarget);
		tc.copy("tableName", "sortColumn");
	}
}
