package asl.seedsplitter;

/**
 * @author Joel D. Edwards
 * 
 */
public class IllegalSampleRateException extends Exception {
	public static final long serialVersionUID = 1L;

  public IllegalSampleRateException(String message) {
		super(message);
	}
}
