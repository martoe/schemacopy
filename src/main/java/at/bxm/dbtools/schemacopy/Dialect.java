package at.bxm.dbtools.schemacopy;

public enum Dialect {

	H2, ORACLE;

	public static Dialect fromString(String value) {
		for (Dialect dialect : Dialect.values()) {
			if (dialect.name().equalsIgnoreCase(value)) {
				return dialect;
			}
		}
		return null;
	}

}
