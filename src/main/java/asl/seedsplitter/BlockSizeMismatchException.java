package asl.seedsplitter;

/**
 * @author Joel D. Edwards
 * <p>
 * The BlockSizeMismatchException is thrown by BlockPool when an attempt is made to add a block of
 * data whose size does not match that determined during instantiation.
 */
class BlockSizeMismatchException extends Exception {

  public static final long serialVersionUID = 1L;

  /**
   * Constructor which accepts a description of the exception.
   *
   * @param message A description of the exception.
   */
  BlockSizeMismatchException(String message) {
    super(message);
  }
}
