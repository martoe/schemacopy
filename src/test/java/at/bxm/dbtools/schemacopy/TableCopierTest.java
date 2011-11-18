package at.bxm.dbtools.schemacopy;

import static org.junit.Assert.*;
import javax.sql.DataSource;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public class TableCopierTest {

    @Test
    public void copy() {
        JdbcTemplate jtSource = createDatabase("source");
        final int datasets = 1111;
        for (int i = 0; i < datasets; i++) {
            jtSource.execute("insert into testtable (id, text) values (" + i +", 'value " + i + "')");
        }
        assertEquals(datasets, jtSource.queryForLong("select count(1) from testtable"));

        JdbcTemplate jtTarget = createDatabase("target");
        assertEquals(0, jtTarget.queryForLong("select count(1) from testtable"));

        TableCopier tc = new TableCopier();
        tc.setSource(jtSource.getDataSource());
        tc.setTarget(jtTarget.getDataSource());
        tc.copy("testtable", "id");
        assertEquals(datasets, jtTarget.queryForLong("select count(1) from testtable"));
    }

    private JdbcTemplate createDatabase(String name) {
        DataSource datasource = new DriverManagerDataSource("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1", "sa", "");
        JdbcTemplate jdbcTemplate = new JdbcTemplate(datasource);
        jdbcTemplate.execute("create table testtable(id number not null, text varchar(100), primary key (id))");
        return jdbcTemplate;
    }

}
