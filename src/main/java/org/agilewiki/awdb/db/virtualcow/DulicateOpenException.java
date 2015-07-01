package org.agilewiki.awdb.db.virtualcow;

import org.agilewiki.awdb.db.BlockIOException;

/**
 * Thrown when opening the already open database.
 */
public class DulicateOpenException extends BlockIOException {
}
