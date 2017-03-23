package simpledb.buffer;

/**
 * A runtime exception indicating that the transaction
 * needs to abort because a simpledb.buffer request could not be satisfied.
 * @author Edward Sciore
 */
@SuppressWarnings("serial")
public class BufferAbortException extends RuntimeException {}
