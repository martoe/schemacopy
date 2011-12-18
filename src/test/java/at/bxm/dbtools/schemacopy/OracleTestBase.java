package at.bxm.dbtools.schemacopy;

import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
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
public class OracleTestBase {

	protected static final String USERNAME_SOURCE = "sourcetest";
	protected static final String USERNAME_TARGET = "targettest";
	protected JdbcTemplate jtSource;
	protected JdbcTemplate jtTarget;

	protected JdbcTemplate connect(String username) {
		DataSource datasource = new DriverManagerDataSource("jdbc:oracle:thin:@localhost:1521:XE", username, "test");
		return new JdbcTemplate(datasource);
	}

}
