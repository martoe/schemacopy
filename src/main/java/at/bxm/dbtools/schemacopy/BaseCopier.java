package at.bxm.dbtools.schemacopy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseCopier {

	protected final Logger logger = LoggerFactory.getLogger(getClass());
	protected static final Logger sqlLogger = LoggerFactory.getLogger(BaseCopier.class.getPackage().getName() + ".SQL");
	protected Database source;
	protected Database target;

	public void setSource(Database value) {
		source = value;
	}

	public void setTarget(Database value) {
		target = value;
	}

}
