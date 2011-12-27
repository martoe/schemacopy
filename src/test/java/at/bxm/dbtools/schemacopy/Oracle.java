package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;

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
	protected static final String USERNAME_SOURCE = "sourcetest";
	protected static final String USERNAME_TARGET = "targettest";
	protected static final String TABLE_NAME = "testtable";
	protected static final String TABLE_COUNTQUERY = "select count(1) from " + TABLE_NAME;
	protected static final String LOBTABLE_NAME = "lobtable";
	protected static final String LOBTABLE_COUNTQUERY = "select count(1) from " + LOBTABLE_NAME;
	protected static final String SEQ_NAME = "seq_test";
	protected static final String SEQ_NEXTVALUE = "select " + SEQ_NAME + ".nextval from dual";

	protected static Database connect(String username) {
		DataSource datasource = new DriverManagerDataSource("jdbc:oracle:thin:@localhost:1521:XE", username, "test");
		return new Database(datasource, Dialect.ORACLE, null);
	}

	protected static Database createSimpleTable(String databaseName) {
		Database database = connect(databaseName);
		database.execute("create table " + TABLE_NAME + "(" +
			"c_id number not null, " +
			"c_text varchar2(100) not null, " +
			"c_number number(16,2) not null, " +
			"c_date timestamp not null, " +
			"primary key (c_id))");
		return database;
	}

	protected static Database createSimpleTableWithData(String databaseName, int datasets) {
		Database datasource = createSimpleTable(databaseName);
		final PreparedStatementSetter pss = new PreparedStatementSetter() {
			private int count = 0;

			@Override
			public void setValues(PreparedStatement ps) throws SQLException {
				ps.setInt(1, ++count);
				ps.setString(2, "some text");
				ps.setDouble(3, (double)count / 3);
				ps.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
			}
		};
		for (int i = 0; i < datasets; i++) {
			datasource.getTemplate()
				.update("insert into " + TABLE_NAME + " (c_id, c_text, c_number, c_date) values (?, ?, ?, ?)", pss);
		}
		assertEquals(datasets, datasource.queryForLong(TABLE_COUNTQUERY));
		return datasource;
	}

	protected static Database createLobTable(String username) {
		Database database = connect(username);
		database.execute("create table " + LOBTABLE_NAME + "(" +
			"c_id number not null, " +
			"c_clob clob not null, " +
			"c_blob blob not null, " +
			"primary key (c_id))");
		return database;
	}

	protected static Database createLobTableWithData(String username, int datasets) {
		Database database = createLobTable(USERNAME_SOURCE);
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
			database.getTemplate().update("insert into " + LOBTABLE_NAME + " (c_id, c_clob, c_blob) values (?, ?, ?)", pss);
		}
		assertEquals(datasets, database.queryForLong(LOBTABLE_COUNTQUERY));
		return database;
	}

	protected static Database createSequence(String username, int start, int increment) {
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
