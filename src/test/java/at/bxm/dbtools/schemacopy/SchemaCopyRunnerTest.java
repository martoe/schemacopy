package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;

public class SchemaCopyRunnerTest {

	@Test
	public void main() throws IOException {
		// mostly cut-paste from TableCopierH2Test :(
		JdbcTemplate jtSource = TableCopierH2Test.createSimpleTable("source");
		final int datasets = 1111;
		final PreparedStatementSetter pss = new PreparedStatementSetter() {
			private int count = 0;

			public void setValues(PreparedStatement ps) throws SQLException {
				ps.setInt(1, ++count);
				ps.setString(2, "some text");
				ps.setDouble(3, (double) count / 3);
				ps.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
			}
		};
		for (int i = 0; i < datasets; i++) {
			jtSource.update("insert into testtable (c_id, c_text, c_number, c_date) values (?, ?, ?, ?)", pss);
		}
		assertEquals(datasets, jtSource.queryForLong(TableCopierH2Test.COUNT_QUERY));

		JdbcTemplate jtTarget = TableCopierH2Test.createSimpleTable("target");
		assertEquals(0, jtTarget.queryForLong(TableCopierH2Test.COUNT_QUERY));

		SchemaCopyRunner.main(null);
		assertEquals(datasets, jtTarget.queryForLong(TableCopierH2Test.COUNT_QUERY));
	}

}
