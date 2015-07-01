package org.agilewiki.awdb.db.virtualcow;

/**
 * Thrown when a block is read with a checksum that differs from what was expected.
 * Closes the database only if this occurs while a transaction occurs.
 * <p>
 *     Useful for optimistic queries.
 * </p>
 */
public class UnexpectedChecksumException extends RuntimeException {
}
