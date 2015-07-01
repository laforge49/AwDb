package org.agilewiki.awdb.db.virtualcow;

import org.agilewiki.awdb.db.immutable.CascadingRegistry;
import org.agilewiki.awdb.db.immutable.collections.*;

/**
 * Initialize the factory registry.
 */
public class DbFactoryRegistry extends CascadingRegistry {
    public final Db db;

    public final VersionedListNode versionedNilList;
    public final VersionedMapNode versionedNilMap;
    public final ListNode nilList;
    public final MapNode nilMap;

    public final VersionedListNodeFactory versionedListNodeFactory;
    public final VersionedMapNodeFactory versionedMapNodeFactory;
    public final ListNodeFactory listNodeFactory;
    public final MapNodeFactory mapNodeFactory;
    public final BlockReferenceFactory blockReferenceFactory;

    public final char versionedListNodeImplId = 'l';
    public final char versionedNilListId = '1';
    public final char versionedListReferenceId = '5';
    public final char versionedMapNodeImplId = 'm';
    public final char versionedNilMapId = '2';
    public final char versionedMapReferenceId = '6';
    public final char listNodeImplId = 'n';
    public final char nilListId = '3';
    public final char listReferenceId = '7';
    public final char mapNodeImplId = 'o';
    public final char nilMapId = '4';
    public final char mapReferenceId = '8';
    public final char blockReferenceFactoryId = 'r';

    /**
     * Create a cascading factory registry.
     *
     * @param db     The database.
     * @param parent The parent registry.
     */
    public DbFactoryRegistry(Db db, CascadingRegistry parent) {
        super(parent);
        this.db = db;
        versionedListNodeFactory = new VersionedListNodeFactory(this);
        versionedNilList = versionedListNodeFactory.versionedNilList;
        versionedMapNodeFactory = new VersionedMapNodeFactory(this);
        versionedNilMap = versionedMapNodeFactory.versionedNilMap;
        listNodeFactory = new ListNodeFactory(this);
        nilList = listNodeFactory.nilList;
        mapNodeFactory = new MapNodeFactory(this);
        nilMap = mapNodeFactory.nilMap;
        blockReferenceFactory = new BlockReferenceFactory(this);
    }
}
