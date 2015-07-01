package org.agilewiki.awdb.db.immutable.collections;

import org.agilewiki.awdb.db.BlockIOException;

/**
 * Thrown when the number of bytes serialized is not the durable length.
 */
public class SerializationException extends BlockIOException {
}
