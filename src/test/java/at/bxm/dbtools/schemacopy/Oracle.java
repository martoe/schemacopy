package at.bxm.dbtools.schemacopy;

import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

// preparing Oracle XE 11g:
//
// connect system/manager
// alter system set processes=150 scope=spfile;
// [restart database]
//
// create user sourcetest identified by test default tablespace users;
// grant create session to sourcetest;
// grant create table to sourcetest;
// grant unlimited tablespace to sourcetest;
// grant create sequence to sourcetest;
//
// create user targettest identified by test default tablespace users;
// grant create session to targettest;
// grant create table to targettest;
// grant unlimited tablespace to targettest;
// grant create sequence to targettest;
public final class Oracle {

	private static final Logger logger = LoggerFactory.getLogger(Oracle.class);
	public static final String TABLE_NAME = DatabaseUtils.TABLE_NAME;
	public static final String TABLE_COUNTQUERY = DatabaseUtils.TABLE_COUNTQUERY;
	public static final String USERNAME_SOURCE = "sourcetest";
	public static final String USERNAME_TARGET = "targettest";
	public static final String SEQ_NAME = "seq_test";
	public static final String SEQ_NEXTVALUE = "select " + SEQ_NAME + ".nextval from dual";

	public static Database connect(String username) {
		DataSource datasource = new DriverManagerDataSource("jdbc:oracle:thin:@localhost:1521:XE", username, "test");
		return new Database(datasource, Dialect.ORACLE, null);
	}

	public static Database createTable(String databaseName) {
		Database database = connect(databaseName);
		database.execute("create table " + TABLE_NAME + "(" +
			"c_int integer not null, " +
			"c_long long not null, " +
			"c_text varchar2(100) not null, " +
			"c_decimal number(16,2) not null, " +
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

	public static Database createSequence(String username, int start, int increment) {
		Database database = connect(username);
		try {
			database.execute("drop sequence " + SEQ_NAME);
		} catch (BadSqlGrammarException ignore) {
		}
		database.execute("create sequence " + SEQ_NAME + " start with " + start + " increment by " + increment);
		//database.execute("select next value for " + SEQ_NAME); // move the sequence to the start value
		//		assertEquals(start, database.queryForLong(SequenceAdjuster.CURRVALUE_QUERY, SEQ_NAME));
		//		assertEquals(increment, database.queryForLong(SequenceAdjuster.INCREMENT_QUERY, SEQ_NAME));
		return database;
	}

	protected static void dropTable(Database database, String tableName) {
		try {
			database.execute("drop table " + tableName);
		} catch (BadSqlGrammarException ignore) {
			logger.info("Could not drop table " + tableName + " - maybe it doesn't exist");
		}
	}

}
