package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import javax.sql.DataSource;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;

public final class H2 {

	public static final String TABLE_NAME = "testtable";
	public static final String TABLE_COUNTQUERY = "select count(1) from " + TABLE_NAME;
	public static final String SEQ_NAME = "seq_test";
	public static final String SEQ_NEXTVALUE = "select next value for " + SEQ_NAME;

	public static Database createInMemoryDatabase(String name) {
		// keep the database as long as the VM lives: DB_CLOSE_DELAY=-1
		// TRACE_LEVEL_SYSTEM_OUT: 0=none, 1=error, 2=info, 3=debug
		DataSource datasource = new DriverManagerDataSource(
			"jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1;TRACE_LEVEL_SYSTEM_OUT=1", "sa", "");
		return new Database(datasource, Dialect.H2, null);
	}

	/** create a table that contains all supported datatypes */
	public static Database createTable(String databaseName) {
		Database database = createInMemoryDatabase(databaseName);
		database.execute("create table " + TABLE_NAME + "(" +
			"c_int integer not null, " +
			"c_long long not null, " +
			"c_text varchar(100) not null, " +
			"c_decimal number not null, " +
			"c_date date not null, " +
			"c_timestamp timestamp not null, " +
			"c_clob clob not null, " +
			"c_blob blob not null, " +
			"primary key (c_int))");
		return database;
	}

	public static Database createTableWithData(String databaseName, int datasets) {
		Database datasource = createTable(databaseName);
		final LobCreator lobCreator = new DefaultLobHandler().getLobCreator();
		final PreparedStatementSetter pss = new PreparedStatementSetter() {
			private int count = 0;

			@Override
			public void setValues(PreparedStatement ps) throws SQLException {
				int i = 0;
				ps.setInt(++i, ++count);
				ps.setLong(++i, ++count);
				ps.setString(++i, "some text");
				ps.setDouble(++i, (double)count / 3);
				ps.setDate(++i, new Date(System.currentTimeMillis()));
				ps.setTimestamp(++i, new Timestamp(System.currentTimeMillis()));
				lobCreator.setClobAsString(ps, ++i, "some very very long text");
				lobCreator.setBlobAsBytes(ps, ++i, "some very big pile of bytes".getBytes());
			}
		};
		for (int i = 0; i < datasets; i++) {
			datasource.getTemplate().update(
				"insert into " + TABLE_NAME + " (c_int, c_long, c_text, c_decimal, c_date, c_timestamp, c_clob, c_blob)" +
					" values (?, ?, ?, ?, ?, ?, ?, ?)",
				pss);
		}
		assertEquals(datasets, datasource.queryForLong(TABLE_COUNTQUERY));
		return datasource;
	}

	public static Database createSequence(String databaseName, int start, int increment) {
		Database database = createInMemoryDatabase(databaseName);
		database.execute("create sequence " + SEQ_NAME + " start with " + start + " increment by " + increment);
		assertEquals(start, database.queryForLong(SEQ_NEXTVALUE)); // move the sequence to the start value
		return database;
	}

	public static int getColumnCount(Database database, String tableName) {
		return -1; // FIXME implement
	}

}
