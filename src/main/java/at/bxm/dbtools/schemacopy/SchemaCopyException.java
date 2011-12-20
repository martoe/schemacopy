package at.bxm.dbtools.schemacopy;

public class SchemaCopyException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public SchemaCopyException(String message) {
		super(message);
	}

	public SchemaCopyException(String message, Throwable cause) {
		super(message, cause);
	}

}
