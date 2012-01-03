package at.bxm.dbtools.schemacopy;

import static at.bxm.dbtools.schemacopy.ArgumentChecker.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseCopier {

	protected final Logger logger = LoggerFactory.getLogger(getClass());
	protected final Database source;
	protected final Database target;

	protected BaseCopier(Database source, Database target) {
		checkNotNull(source, "source");
		checkNotNull(target, "target");
		this.source = source;
		this.target = target;
	}

}
