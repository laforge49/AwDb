package org.agilewiki.awdb.db.virtualcow;

import org.agilewiki.awdb.db.BlockIOException;

/**
 * Thrown when a privileged operation is attenpted outside of a transaction.
 */
public class PrivilegedOperationException extends BlockIOException {
}
