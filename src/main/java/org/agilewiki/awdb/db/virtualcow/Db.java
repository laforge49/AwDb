package org.agilewiki.awdb.db.virtualcow;

import org.agilewiki.jactor2.core.blades.IsolationBladeBase;
import org.agilewiki.jactor2.core.messages.AsyncResponseProcessor;
import org.agilewiki.jactor2.core.messages.impl.AsyncRequestImpl;
import org.agilewiki.awdb.db.BlockIOException;
import org.agilewiki.awdb.db.dsm.DiskSpaceManager;
import org.agilewiki.awdb.db.ids.Timestamp;
import org.agilewiki.awdb.db.ids.ValueId;
import org.agilewiki.awdb.db.ids.composites.Journal;
import org.agilewiki.awdb.db.immutable.CascadingRegistry;
import org.agilewiki.awdb.db.immutable.ImmutableFactory;
import org.agilewiki.awdb.db.immutable.collections.*;
import org.agilewiki.awdb.db.immutable.scalars.CS256;
import org.agilewiki.awdb.db.immutable.scalars.CS256Factory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.file.StandardOpenOption.*;

/**
 * A database that supports multiple blocks.
 */
public class Db extends IsolationBladeBase implements AutoCloseable {
    public final static String transactionNameId = "$ntransactionName";

    public final ConcurrentHashMap<String, Class> transactionRegistry =
            new ConcurrentHashMap<>(16, 0.75f, 1);
    public final DbFactoryRegistry dbFactoryRegistry;
    public final Path dbPath;
    private FileChannel fc;
    public final int maxBlockSize;
    private long nextRootPosition;
    private MapNode mapNode;
    private MapNode dbMapNode;
    protected Thread privilegedThread;
    private DiskSpaceManager dsm;
    private long timestamp;
    private String jeName;

    /**
     * Create a Db actor.
     *
     * @param parentRegistry The parent cascading registry.
     * @param dbPath         The path of the db file.
     * @param maxBlockSize   The maximum root block size.
     */
    public Db(CascadingRegistry parentRegistry,
              Path dbPath,
              int maxBlockSize) throws Exception {
        dbFactoryRegistry = new DbFactoryRegistry(this, parentRegistry);
        this.dbPath = dbPath;
        this.maxBlockSize = maxBlockSize;
        timestamp = Timestamp.generate();
    }

    /**
     * Register a transaction class.
     *
     * @param transactionName  The transaction name.
     * @param transactionClass The transaction class.
     */
    public void registerTransaction(String transactionName, Class transactionClass) {
        transactionRegistry.put(transactionName, transactionClass);
    }

    /**
     * Returns the contents of the database.
     *
     * @return A MapAccessor.
     */
    public MapAccessor mapAccessor() {
        return (isPrivileged() ? dbMapNode : mapNode).mapAccessor();
    }

    /**
     * Returns a Versioned Map Node (VMM) for the given id.
     *
     * @param id The id of the VMN.
     * @return The VMN, or null.
     */
    public VersionedMapNode versionedMapNode(String id) {
        ListAccessor listAccessor = mapAccessor().listAccessor(id);
        if (listAccessor == null)
            return null;
        return (VersionedMapNode) listAccessor.get(0);
    }

    /**
     * Returns the versioned list node for a given id and key.
     *
     * @param id  The id for the versioned list node.
     * @param key The key for the versioned list node.
     * @return The versioned list node, or null.
     */
    public VersionedListNode versionedListNode(String id, String key) {
        VersionedMapNode versionedMapNode = versionedMapNode(id);
        if (versionedMapNode == null)
            return null;
        return versionedMapNode.getList(key);
    }

    public void updateJournal(String id) {
        set(Journal.modifiesId(jeName), id, true, true);
        set(Journal.journalId(id), jeName, true, true);
    }

    /**
     * Iterates over the keys under an id.
     *
     * @param id The id of a VMN.
     * @return The key iterable.
     */
    public PeekABoo<String> keysIterable(String id, long timestamp) {
        ValueId.validateAnId(id);
        MapAccessor ma = mapAccessor();
        ListAccessor la = ma.listAccessor(id);
        if (la == null) {
            return new EmptyPeekABoo();
        }
        VersionedMapNode vmn = (VersionedMapNode) la.get(0);
        PeekABoo<ListAccessor> lait = vmn.iterator(timestamp);
        return new ListAccessorKeysMap(lait);
    }

    /**
     * Iterates over the non-empty Ids.
     *
     * @param prefix       The prefix of the ids.
     * @param timestamp    The time of the query.
     * @return The key iterable.
     */
    public PeekABoo<String> idsIterable(String prefix, long timestamp) {
        return new IdPeekABooable(this, prefix, timestamp).iterator();
    }

    /**
     * Clear the versioned map.
     *
     * @param id The id for the VersionedMapNode.
     */
    public void clearMap(String id) {
        checkPrivilege();
        if (!id.startsWith("$"))
            throw new IllegalArgumentException("not an id or composite id: " + id);
        ListNode listNode = dbMapNode.getList(id);
        if (listNode == null)
            return;
        VersionedMapNode versionedMapNode = (VersionedMapNode) listNode.get(0);
        versionedMapNode = versionedMapNode.clearMap();
        dbMapNode = dbMapNode.set(id, versionedMapNode);
        updateJournal(id);
    }

    /**
     * Add a non-null value to the end of the list.
     *
     * @param id    The id of the list.
     * @param value The value to be added.
     */
    public void add(String id, Object value) {
        add(id, -1, value);
    }

    /**
     * Add a non-null value to the list.
     *
     * @param id    The id of the list.
     * @param ndx   Where to add the value.
     * @param value The value to be added.
     */
    public void add(String id, int ndx, Object value) {
        checkPrivilege();
        dbMapNode = dbMapNode.add(id, ndx, value);
    }

    /**
     * Clear the versioned list.
     *
     * @param id  The id for the VersionedMapNode.
     * @param key The key for the VersionedListNode in the VersionedMapNode.
     */
    public void clearList(String id, String key) {
        checkPrivilege();
        if (!id.startsWith("$"))
            throw new IllegalArgumentException("not an id or composite id: " + id);
        ListNode listNode = dbMapNode.getList(id);
        if (listNode == null)
            return;
        VersionedMapNode versionedMapNode = (VersionedMapNode) listNode.get(0);
        versionedMapNode = versionedMapNode.clearList(key);
        dbMapNode = dbMapNode.set(id, versionedMapNode);
        updateJournal(id);
    }

    /**
     * Remove a list if empty.
     *
     * @param id The id of the list.
     */
    public void removeIfEmpty(String id) {
        checkPrivilege();
        if (!id.startsWith("$"))
            throw new IllegalArgumentException("not an id or composite id: " + id);
        ListNode listNode = dbMapNode.getList(id);
        if (listNode == null)
            return;
        if (!listNode.isEmpty())
            return;
        dbMapNode = dbMapNode.remove(id);
    }

    /**
     * Remove an item from a list node.
     *
     * @param id    The id of the list node.
     * @param value The value to be removed.
     */
    public void remove(String id, Object value) {
        checkPrivilege();
        if (!id.startsWith("$"))
            throw new IllegalArgumentException("not an id or composite id: " + id);
        ListNode listNode = dbMapNode.getList(id);
        if (listNode == null)
            return;
        if (!listNode.isEmpty())
            return;
        dbMapNode.remove(id, value);
    }

    /**
     * Remove an item from a versioned list.
     *
     * @param id  The id for the VersionedMapNode.
     * @param key The key for the VersionedListNode in the VersionedMapNode.
     * @param ndx The index of the item to be deleted.
     */
    public void remove(String id, String key, int ndx) {
        checkPrivilege();
        if (!id.startsWith("$"))
            throw new IllegalArgumentException("not an id or composite id: " + id);
        ListNode listNode = dbMapNode.getList(id);
        if (listNode == null)
            return;
        VersionedMapNode versionedMapNode = (VersionedMapNode) listNode.get(0);
        versionedMapNode = versionedMapNode.remove(key, ndx);
        dbMapNode = dbMapNode.set(id, versionedMapNode);
        updateJournal(id);
    }

    /**
     * Remove the first occurance of an item from a versioned list.
     *
     * @param id  The id for the VersionedMapNode.
     * @param key The key for the VersionedListNode in the VersionedMapNode.
     * @param x   The item to be deleted.
     */
    public void remove(String id, String key, Object x) {
        checkPrivilege();
        if (!id.startsWith("$"))
            throw new IllegalArgumentException("not an id or composite id: " + id);
        ListNode listNode = dbMapNode.getList(id);
        if (listNode == null)
            return;
        VersionedMapNode versionedMapNode = (VersionedMapNode) listNode.get(0);
        versionedMapNode = versionedMapNode.remove(key, x);
        dbMapNode = dbMapNode.set(id, versionedMapNode);
        updateJournal(id);
    }

    /**
     * Set an item in a versioned list.
     *
     * @param id    The id for the VersionedMapNode.
     * @param key   The key for the VersionedListNode in the VersionedMapNode.
     * @param value The new value.
     */
    public void set(String id, String key, Object value) {
        set(id, key, value, false);
    }

    public void set(String id, String key, Object value, boolean journal) {
        checkPrivilege();
        if (!id.startsWith("$"))
            throw new IllegalArgumentException("not an id or composite id: " + id);
        ListNode listNode = dbMapNode.getList(id);
        VersionedMapNode versionedMapNode = listNode == null ?
                dbFactoryRegistry.versionedNilMap :
                (VersionedMapNode) listNode.get(0);
        versionedMapNode = versionedMapNode.set(key, value);
        dbMapNode = dbMapNode.set(id, versionedMapNode);
        if (!journal)
            updateJournal(id);
    }

    /**
     * Add an item to the end of a versioned list.
     *
     * @param id    The id for the VersionedMapNode.
     * @param key   The key for the VersionedListNode in the VersionedMapNode.
     * @param value The value to be added to the list.
     */
    public void add(String id, String key, Object value) {
        add(id, key, value, false);
    }

    private void add(String id, String key, Object value, boolean journal) {
        checkPrivilege();
        if (!id.startsWith("$"))
            throw new IllegalArgumentException("not an id or composite id: " + id);
        ListNode listNode = dbMapNode.getList(id);
        VersionedMapNode versionedMapNode = listNode == null ?
                dbFactoryRegistry.versionedNilMap :
                (VersionedMapNode) listNode.get(0);
        versionedMapNode = versionedMapNode.add(key, value);
        dbMapNode = dbMapNode.set(id, versionedMapNode);
        if (!journal)
            updateJournal(id);
    }

    /**
     * Add an item to the end of a versioned list.
     *
     * @param id    The id for the VersionedMapNode.
     * @param key   The key for the VersionedListNode in the VersionedMapNode.
     * @param ndx   Where to add the value.
     * @param value The value to be added to the list.
     */
    public void add(String id, String key, int ndx, Object value) {
        checkPrivilege();
        if (!id.startsWith("$"))
            throw new IllegalArgumentException("not an id or composite id: " + id);
        ListNode listNode = dbMapNode.getList(id);
        VersionedMapNode versionedMapNode = listNode == null ?
                dbFactoryRegistry.versionedNilMap :
                (VersionedMapNode) listNode.get(0);
        versionedMapNode = versionedMapNode.add(key, ndx, value);
        dbMapNode = dbMapNode.set(id, versionedMapNode);
        updateJournal(id);
    }

    /**
     * In the form (currentTimeMillis() &lt;&lt; 10) | index,
     * the timestamp reflects the time this object was created
     * or the time of the last transaction.
     * (The index is added to make the timestamp unique.)
     *
     * @return The latest timestamp.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Test method to set the timestamp when the file is not open.
     *
     * @param timestamp The new timestamp.
     */
    public void _setTimestamp(long timestamp) {
        if (fc != null)
            throw new UnsupportedOperationException("not valid when db is open");
        this.timestamp = timestamp;
    }

    /**
     * Open the db, creating a new db file.
     *
     * @param createNew True when a db file must not already exist.
     */
    public void open(boolean createNew) {
        if (fc != null) {
            close();
            getReactor().error("open on already open db");
            throw new DulicateOpenException();
        }
        mapNode = null;
        try {
            if (createNew)
                fc = FileChannel.open(dbPath, READ, WRITE, SYNC, CREATE_NEW);
            else
                fc = FileChannel.open(dbPath, READ, WRITE, SYNC, CREATE);
            dsm = new DiskSpaceManager();
            dsm.allocate();
            dsm.allocate();
            mapNode = null;
            dbMapNode = dbFactoryRegistry.nilMap;
            _update();
            mapNode = null;
            dbMapNode = dbFactoryRegistry.nilMap;
            _update();
        } catch (IOException ex) {
            close();
            getReactor().error("unable to open db to create a new file", ex);
            throw new BlockIOException(ex);
        }
    }

    /**
     * Update the database with a parameter-free transaction.
     *
     * @param transactionName The registered name of the transaction class.
     * @return The request to perform the update.
     */
    public AReq<String> update(String transactionName) {
        MapNode tMapNode = dbFactoryRegistry.nilMap;
        return update(transactionName, dbFactoryRegistry.nilMap);
    }

    /**
     * Update the database.
     *
     * @param transactionName The registered name of the transaction class.
     * @param tMapNode        The map holding the transaction parameters.
     * @return The request to perform the update.
     */
    public AReq<String> update(String transactionName, MapNode tMapNode) {
        tMapNode = tMapNode.add(Db.transactionNameId, transactionName);
        return update(tMapNode.toByteBuffer());
    }

    /**
     * Update the database.
     * First deserialize the map list held by the byte buffer, then fetch the name of the
     * transaction class assigned to the key transaction_class_name.
     * Then instantiate the transaction and call the transform method.
     * The database is updated on successful completion of the transaction.
     *
     * @param tByteBuffer Holds the serialized transaction which will transform the db contents.
     * @return The request to perform the update.
     */
    public AReq<String> update(ByteBuffer tByteBuffer) {
        return new AReq<String>("update") {
            @Override
            protected void processAsyncOperation(AsyncRequestImpl _asyncRequestImpl,
                                                 AsyncResponseProcessor<String> _asyncResponseProcessor) {
                try {
                    ImmutableFactory f = dbFactoryRegistry.readId(tByteBuffer);
                    MapNode tMapNode = (MapNode) f.deserialize(tByteBuffer);
                    String transactionName = (String) tMapNode.getList(Db.transactionNameId).get(0);
                    Class tClass = transactionRegistry.get(transactionName);
                    Transaction transaction = (Transaction) tClass.newInstance();
                    _asyncRequestImpl.setMessageTimeoutMillis(transaction.timeoutMillis());
                    privilegedThread = Thread.currentThread();
                    try {
                        timestamp = Timestamp.generate();
                        dbMapNode = mapNode;
                        VersionedMapNode je = dbFactoryRegistry.versionedNilMap;
                        jeName = Timestamp.timestampId(timestamp);
                        MapAccessor ma = tMapNode.mapAccessor();
                        for (ListAccessor la : ma) {
                            String key = (String) la.key();
                            for (Object v : la) {
                                je = je.add(key, v);
                            }
                        }
                        dbMapNode = dbMapNode.add(jeName, je);
                        transaction.transform(Db.this, tMapNode);
                        _update();
                    } finally {
                        privilegedThread = null;
                    }
                    _asyncResponseProcessor.processAsyncResponse(jeName);
                } catch (Exception ex) {
                    close();
                    getReactor().error("unable to update db", ex);
                    throw new BlockIOException(ex);
                }
            }
        };
    }

    /**
     * Returns the name of the journal entry being processed.
     *
     * @return The current journal entry name.
     */
    public String getJEName() {
        return jeName;
    }

    /**
     * Verifies that the thread is processing a transaction.
     * Otherwise an IllegalStateException is thrown.
     */
    public void checkPrivilege() {
        if (!isPrivileged())
            throw new PrivilegedOperationException();
    }

    /**
     * Returns true if the thread is privileged.
     *
     * @return True if a transaction is being processed.
     */
    public boolean isPrivileged() {
        return Thread.currentThread() == privilegedThread;
    }

    protected void _update() {
        if (dbMapNode == mapNode)
            return; // Query?
        ImmutableFactory factory = dbFactoryRegistry.getImmutableFactory(dbMapNode);
        int dsmLength = dsm.durableLength();
        int maxDurableLength = maxBlockSize - 4 - 4 - 34 - 8 - dsmLength;
        int dl = dbMapNode.getDurableLength();
        while (dl > maxDurableLength) {
            dbMapNode = (MapNode) dbMapNode.resize(maxDurableLength, maxBlockSize);
            dsmLength = dsm.durableLength(); // may have grown
            maxDurableLength = maxBlockSize - 4 - 4 - 34 - 8 - dsmLength;
            dl = dbMapNode.getDurableLength();
        }
        dsm.commit();
        dsmLength = dsm.durableLength(); // may have shrunk
        dl = dbMapNode.getDurableLength();
        int contentSize = 8 + dsmLength + dl;
        int blockSize = 4 + 4 + 34 + contentSize;
        if (blockSize > maxBlockSize) {
            close();
            throw new MaxBlockSizeTooSmallException();
        }
        ByteBuffer contentBuffer = ByteBuffer.allocate(contentSize);
        contentBuffer.putLong(timestamp);
        dsm.write(contentBuffer);
        factory.writeDurable(dbMapNode, contentBuffer);
        contentBuffer.flip();
        CS256 cs256 = new CS256(contentBuffer);
        ByteBuffer byteBuffer = ByteBuffer.allocate(blockSize);
        byteBuffer.putInt(maxBlockSize);
        byteBuffer.putInt(blockSize);
        ImmutableFactory cs256Factory = dbFactoryRegistry.getImmutableFactory(cs256);
        cs256Factory.writeDurable(cs256, byteBuffer);
        byteBuffer.put(contentBuffer);
        byteBuffer.flip();
        long p = nextRootPosition;
        try {
            while (byteBuffer.remaining() > 0) {
                p += fc.write(byteBuffer, p);
            }
        } catch (IOException ex) {
            close();
            throw new BlockIOException(ex);
        }
        nextRootPosition = (nextRootPosition + maxBlockSize) % (2 * maxBlockSize);
        mapNode = dbMapNode;
        return;
    }

    public void readBlock(ByteBuffer byteBuffer, int blockNbr) {
        long position = blockNbr * (long) maxBlockSize;
        try {
            while (byteBuffer.remaining() > 0) {
                position += fc.read(byteBuffer, position);
            }
        } catch (IOException ex) {
            close();
            throw new BlockIOException(ex);
        }
    }

    public void writeBlock(ByteBuffer byteBuffer, int blockNbr) {
        checkPrivilege();
        long position = blockNbr * (long) maxBlockSize;
        try {
            while (byteBuffer.remaining() > 0) {
                position += fc.write(byteBuffer, position);
            }
        } catch (IOException ex) {
            close();
            throw new BlockIOException(ex);
        }
    }

    @Override
    public void close() {
        if (fc != null) {
            try {
                fc.close();
            } catch (IOException ex) {
                throw new BlockIOException(ex);
            }
            fc = null;
        }
    }

    /**
     * Open an existing database.
     */
    public void open() {
        if (fc != null) {
            close();
            getReactor().error("open on already open db");
            throw new DulicateOpenException();
        }
        if (Files.notExists(dbPath)) {
            getReactor().error("file does not exist: " + dbPath);
            throw new FileDoesNotExistExcpetion();
        }
        if (!Files.isReadable(dbPath)) {
            getReactor().error("file is not readable: " + dbPath);
            throw new FileNotReadableException();
        }
        if (!Files.isWritable(dbPath)) {
            getReactor().error("file is not writable: " + dbPath);
            throw new FileNotWritableException();
        }
        if (!Files.isRegularFile(dbPath)) {
            getReactor().error("file is not a regular file: " + dbPath);
            throw new FileNotRegularExcpetion();
        }
        try {
            fc = FileChannel.open(dbPath, READ, WRITE, SYNC);
            RootBlock rb0 = readRootBlock(0L);
            RootBlock rb1 = readRootBlock(maxBlockSize);
            if (rb0 == null && rb1 == null) {
                throw new IllegalStateException("no valid root blocks found");
            }
            RootBlock rb;
            if (rb0 == null) {
                rb = rb1;
                nextRootPosition = 0L;
            } else if (rb1 == null) {
                rb = rb0;
                nextRootPosition = maxBlockSize;
            } else if (rb0.timestamp > rb1.timestamp) {
                rb = rb0;
                nextRootPosition = maxBlockSize;
            } else {
                rb = rb1;
                nextRootPosition = 0L;
            }
            dsm = new DiskSpaceManager(rb.serializedContent);
            ImmutableFactory factory = dbFactoryRegistry.readId(rb.serializedContent);
            mapNode = (MapNode) factory.deserialize(rb.serializedContent);
        } catch (IOException ex) {
            close();
            getReactor().error("Unable to open existing db file", ex);
            throw new BlockIOException(ex);
        }
    }

    protected RootBlock readRootBlock(long position) {
        try {
            ByteBuffer header = ByteBuffer.allocate(4 + 4 + 34);
            while (header.remaining() > 0) {
                position += fc.read(header, position);
            }
            header.flip();
            int maxSize = header.getInt();
            if (maxBlockSize != maxSize) {
                getReactor().warn("root block max size is incorrect " + maxSize);
                return null;
            }
            int blockSize = header.getInt();
            if (blockSize < 4 + 4 + 34 + 8 + 4 + 2) {
                getReactor().warn("root block size is too small");
                return null;
            }
            if (blockSize > maxBlockSize) {
                getReactor().warn("root block size exceeds max block size");
                return null;
            }
            ImmutableFactory csf = dbFactoryRegistry.readId(header);
            if (!(csf instanceof CS256Factory)) {
                getReactor().warn("expecting CS256 in root block");
                return null;
            }
            CS256 cs1 = (CS256) csf.deserialize(header);
            ByteBuffer body = ByteBuffer.allocate(blockSize - 4 - 4 - 34);
            while (body.remaining() > 0) {
                position += fc.read(body, position);
            }
            body.flip();
            CS256 cs2 = new CS256(body);
            if (!cs1.equals(cs2)) {
                getReactor().warn("root block has bad checksum");
                return null;
            }
            RootBlock rb = new RootBlock();
            rb.timestamp = body.getLong();
            rb.serializedContent = body;
            return rb;
        } catch (Exception ex) {
            getReactor().warn("unable to read root block", ex);
            return null;
        }
    }

    protected class RootBlock {
        long timestamp;
        ByteBuffer serializedContent;
    }

    /**
     * Allocates a block of disk space.
     * But if not processing a transaction when called,
     * an IllegalStateException is thrown.
     *
     * @return The number of the block that was allocated.
     */
    public int allocate() {
        checkPrivilege();
        return dsm.allocate();
    }

    /**
     * Returns the number of allocated pages.
     * But if not processing a transaction when called,
     * an IllegalStateException is thrown.
     *
     * @return The number of pages in use.
     */
    public int usage() {
        checkPrivilege();
        return dsm.usage();
    }

    /**
     * Release a block.
     * It will become available on the next transaction.
     * But if not processing a transaction when called,
     * an IllegalStateException is thrown.
     *
     * @param i The block to be released.
     */
    public void release(int i) {
        checkPrivilege();
        dsm.release(i);
    }

    /**
     * Get the selected vmn.
     *
     * @param id The id of the selected vmn.
     * @return The vmn, or null.
     */
    public VersionedMapNode get(Comparable id) {
        ListAccessor la = mapAccessor().listAccessor(id);
        if (la == null)
            return null;
        if (la.isEmpty())
            return null;
        return (VersionedMapNode) la.get(0);
    }

    /**
     * Get the selected object.
     *
     * @param id        The id of the selected object.
     * @param key       The key of the selected object.
     * @param timestamp The time of the query.
     * @return The selected object, or null.
     */
    public Object get(Comparable id, Comparable key, long timestamp) {
        VersionedMapNode vmn = get(id);
        if (vmn == null)
            return null;
        return vmn.get(key, timestamp);
    }
}
