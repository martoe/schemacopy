package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;

public final class H2 {

	protected static final String LOBTABLE_COUNTQUERY = "select count(1) from lobtable";

	protected static JdbcTemplate createInMemoryDatabase(String name) {
		// keep the database as long as the VM lives: DB_CLOSE_DELAY=-1 
		DataSource datasource = new DriverManagerDataSource("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1", "sa", "");
		return new JdbcTemplate(datasource);
	}

	protected static JdbcTemplate createLobTable(String databaseName) {
		JdbcTemplate database = createInMemoryDatabase(databaseName);
		database.execute("create table lobtable(" +
			"c_id number not null, " +
			"c_clob clob not null, " +
			"c_blob blob not null, " +
			"primary key (c_id))");
		return database;
	}

	protected static JdbcTemplate createLobTableWithData(String databaseName, int datasets) {
		JdbcTemplate database = createLobTable("source");
		final LobCreator lobCreator = new DefaultLobHandler().getLobCreator();
		final PreparedStatementSetter pss = new PreparedStatementSetter() {
			private int count = 0;

			@Override
			public void setValues(PreparedStatement ps) throws SQLException {
				ps.setInt(1, ++count);
				lobCreator.setClobAsString(ps, 2, "some text");
				lobCreator.setBlobAsBytes(ps, 3, "some bytes".getBytes());
			}
		};
		for (int i = 0; i < datasets; i++) {
			database.update("insert into lobtable (c_id, c_clob, c_blob) values (?, ?, ?)", pss);
		}
		assertEquals(datasets, database.queryForLong(LOBTABLE_COUNTQUERY));
		return database;
	}

	protected static JdbcTemplate createLocalDatabase(File filename) {
		DataSource datasource = new DriverManagerDataSource("jdbc:h2:file:" + filename.getAbsolutePath(),
			Database.LOCAL_USERNAME, Database.LOCAL_PASSWORD);
		return new JdbcTemplate(datasource);
	}

}
