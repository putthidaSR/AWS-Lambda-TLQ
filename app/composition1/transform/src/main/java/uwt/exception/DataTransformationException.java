package uwt.exception;

/**
 * Exception to be thrown to indicate something went wrong with data transformation service.
 */
@SuppressWarnings("serial")
public class DataTransformationException extends RuntimeException {

	public static final String FAIL_DATE_PARSING = "Failed to parse date string to calculate processing days.";
	public static final String FAIL_ORDER_PRIORITY_PARSING = "Failed to transform order priorty value.";
	
	public DataTransformationException(String message) {
        super(message);
    }

    public DataTransformationException(String message, Exception e) {
        super(message, e);
    }

}
