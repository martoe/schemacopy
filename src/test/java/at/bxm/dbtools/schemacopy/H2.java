package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import javax.sql.DataSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public final class H2 {

	public static final String TABLE_NAME = DatabaseUtils.TABLE_NAME;
	public static final String TABLE_COUNTQUERY = DatabaseUtils.TABLE_COUNTQUERY;
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
		Database database = createTable(databaseName);
		DatabaseUtils.createTableData(database, datasets);
		return database;
	}

	public static Database createSequence(String databaseName, int start, int increment) {
		Database database = createInMemoryDatabase(databaseName);
		database.execute("create sequence " + SEQ_NAME + " start with " + start + " increment by " + increment);
		assertEquals(start, database.queryForLong(SEQ_NEXTVALUE)); // move the sequence to the start value
		return database;
	}

}
