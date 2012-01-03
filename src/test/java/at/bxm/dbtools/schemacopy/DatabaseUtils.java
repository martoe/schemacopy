package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;

public final class DatabaseUtils {

	public static final String TABLE_NAME = "testtable";
	public static final String TABLE_COUNTQUERY = "select count(1) from " + TABLE_NAME;

	static void createTableData(Database datasource, int datasets) {
		final LobCreator lobCreator = new DefaultLobHandler().getLobCreator();
		final PreparedStatementSetter pss = new PreparedStatementSetter() {
			private int count = 0;

			@Override
			public void setValues(PreparedStatement ps) throws SQLException {
				int i = 0;
				ps.setInt(++i, ++count);
				ps.setLong(++i, count * 123);
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
	}

	public static int getColumnCount(Database database, String tableName) {
		return database.getTemplate().query("select * from " + tableName, new ResultSetExtractor<Integer>() {

			@Override
			public Integer extractData(ResultSet rs) throws SQLException, DataAccessException {
				return rs.getMetaData().getColumnCount();
			}
		}).intValue();
	}

}
