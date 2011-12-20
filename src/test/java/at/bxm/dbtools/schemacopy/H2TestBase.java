package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import javax.sql.DataSource;
import org.junit.After;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
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
		JdbcTemplate database = createInMemoryDatabase(databaseName);
		database.execute("create table testtable(" +
			"c_id number not null, " +
			"c_text varchar(100) not null, " +
			"c_number number not null, " +
			"c_date timestamp not null, " +
			"primary key (c_id))");
		return database;
	}

	protected final JdbcTemplate createSimpleTableWithData(String databaseName, int datasets) {
		JdbcTemplate datasource = createSimpleTable(databaseName);
		final PreparedStatementSetter pss = new PreparedStatementSetter() {
			private int count = 0;

			@Override
			public void setValues(PreparedStatement ps) throws SQLException {
				ps.setInt(1, ++count);
				ps.setString(2, "some text");
				ps.setDouble(3, (double) count / 3);
				ps.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
			}
		};
		for (int i = 0; i < datasets; i++) {
			datasource.update("insert into testtable (c_id, c_text, c_number, c_date) values (?, ?, ?, ?)", pss);
		}
		assertEquals(datasets, datasource.queryForLong(COUNT_QUERY));
		return datasource;
	}

	protected JdbcTemplate createInMemoryDatabase(String name) {
		// keep the database as long as the VM lives: DB_CLOSE_DELAY=-1 
		DataSource datasource = new DriverManagerDataSource("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1", "sa", "");
		return new JdbcTemplate(datasource);
	}

	protected JdbcTemplate createLocalDatabase(File filename) {
		DataSource datasource = new DriverManagerDataSource("jdbc:h2:file:" + filename.getAbsolutePath(),
			Database.LOCAL_USERNAME, Database.LOCAL_PASSWORD);
		return new JdbcTemplate(datasource);
	}

}
