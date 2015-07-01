/**
 * Disk space management for Copy On Write.
 * <p>
 * Copy On Write means that disk space that has just been freed
 * will not be overwritten until the next transaction.
 * So while a BitSet can be used to manage free space, the space
 * should not become available until the next cycle.
 * </p>
 */
package org.agilewiki.awdb.db.dsm;