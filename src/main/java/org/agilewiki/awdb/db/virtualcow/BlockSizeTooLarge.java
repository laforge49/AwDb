package org.agilewiki.awdb.db.virtualcow;

import org.agilewiki.awdb.db.BlockIOException;

/**
 * Thrown when creating a block that is larger than the max block size.
 */
public class BlockSizeTooLarge extends BlockIOException {
}
