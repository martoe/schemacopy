package at.bxm.dbtools.schemacopy;

public final class ArgumentChecker {

	private ArgumentChecker() {
		throw new UnsupportedOperationException();
	}

	public static void checkNotNull(Object argument, String argumentName) {
		if (argument == null) {
			throw new IllegalArgumentException(argumentName + " must not be null");
		}
	}

	public static void checkGreaterThan(int argument, int value, String argumentName) {
		if (argument <= value) {
			throw new IllegalArgumentException(argumentName + " must be greater than " + value);
		}
	}

}
