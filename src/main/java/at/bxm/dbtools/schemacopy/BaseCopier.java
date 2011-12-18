package at.bxm.dbtools.schemacopy;

import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

public abstract class BaseCopier {

	protected final Logger logger = LoggerFactory.getLogger(getClass());
	protected static final Logger sqlLogger = LoggerFactory.getLogger(BaseCopier.class.getPackage().getName() + ".SQL");
	protected JdbcTemplate source;
	protected JdbcTemplate target;

	public void setSource(DataSource value) {
		source = new JdbcTemplate(value);
	}

	public void setTarget(DataSource value) {
		target = new JdbcTemplate(value);
	}

}
