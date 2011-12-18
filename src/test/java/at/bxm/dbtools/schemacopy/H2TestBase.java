package at.bxm.dbtools.schemacopy;

import javax.sql.DataSource;
import org.junit.After;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public class H2TestBase {

	static final String COUNT_QUERY = "select count(1) from testtable";
	protected JdbcTemplate jtSource;
	protected JdbcTemplate jtTarget;

	@After
	public void dropAll() {
		if (jtSource != null) {
			jtSource.execute("drop all objects");
		}
		if (jtTarget != null) {
			jtTarget.execute("drop all objects");
		}
	}

	protected final JdbcTemplate createSimpleTable(String databaseName) {
		JdbcTemplate database = createDatabase(databaseName);
		database.execute("create table testtable(" +
			"c_id number not null, " +
			"c_text varchar(100) not null, " +
			"c_number number not null, " +
			"c_date timestamp not null, " +
			"primary key (c_id))");
		return database;
	}

	protected JdbcTemplate createDatabase(String name) {
		DataSource datasource = new DriverManagerDataSource("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1", "sa", "");
		return new JdbcTemplate(datasource);
	}

}
