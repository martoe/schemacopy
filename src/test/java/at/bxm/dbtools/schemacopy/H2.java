package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import javax.sql.DataSource;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;

public final class H2 {

	protected static final String TABLE_COUNTQUERY = "select count(1) from testtable";
	protected static final String SEQ_NAME = "seq_test";
	protected static final String SEQ_NEXTVALUE = "select next value for " + SEQ_NAME;

	protected static Database createInMemoryDatabase(String name) {
		// keep the database as long as the VM lives: DB_CLOSE_DELAY=-1 
		DataSource datasource = new DriverManagerDataSource("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1", "sa", "");
		return new Database(datasource, Dialect.H2, null);
	}

	protected static Database createTable(String databaseName) {
		Database database = createInMemoryDatabase(databaseName);
		database.execute("create table testtable(" +
			"c_id number not null, " +
			"c_text varchar(100) not null, " +
			"c_number number not null, " +
			"c_date timestamp not null, " +
			"c_clob clob not null, " +
			"c_blob blob not null, " +
			"primary key (c_id))");
		return database;
	}

	protected static Database createTableWithData(String databaseName, int datasets) {
		Database datasource = createTable(databaseName);
		final LobCreator lobCreator = new DefaultLobHandler().getLobCreator();
		final PreparedStatementSetter pss = new PreparedStatementSetter() {
			private int count = 0;

			@Override
			public void setValues(PreparedStatement ps) throws SQLException {
				ps.setInt(1, ++count);
				ps.setString(2, "some text");
				ps.setDouble(3, (double)count / 3);
				ps.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
				lobCreator.setClobAsString(ps, 5, "some very very long text");
				lobCreator.setBlobAsBytes(ps, 6, "some very big pile of bytes".getBytes());
			}
		};
		for (int i = 0; i < datasets; i++) {
			datasource
				.getTemplate()
				.update("insert into testtable (c_id, c_text, c_number, c_date, c_clob, c_blob) values (?, ?, ?, ?, ?, ?)", pss);
		}
		assertEquals(datasets, datasource.queryForLong(TABLE_COUNTQUERY));
		return datasource;
	}

	protected static Database createSequence(String databaseName, int start, int increment) {
		Database database = createInMemoryDatabase(databaseName);
		database.execute("create sequence " + SEQ_NAME + " start with " + start + " increment by " + increment);
		assertEquals(start, database.queryForLong(SEQ_NEXTVALUE)); // move the sequence to the start value
		return database;
	}

}
