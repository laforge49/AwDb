package org.agilewiki.awdb.db.immutable.collections;

import junit.framework.TestCase;
import org.agilewiki.jactor2.core.impl.Plant;
import org.agilewiki.awdb.db.immutable.BaseRegistry;
import org.agilewiki.awdb.db.virtualcow.Db;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.file.Path;
import java.nio.file.Paths;

public class VersionedMapRemoveTest extends TestCase {
    public void test() throws Exception {
        new Plant();
        try {
            Path dbPath = Paths.get("cow.db");
            int maxRootBlockSize = 10;
            Db db = new Db(new BaseRegistry(), dbPath, maxRootBlockSize);
            DbFactoryRegistry registry = db.dbFactoryRegistry;

            db._setTimestamp(1);
            assertEquals(registry.versionedNilMap, registry.versionedNilMap.remove("", 0));
            registry.versionedNilMap.clearMap();
            assertEquals(1, registry.versionedNilMap.set("1", "a").size(1));

            db._setTimestamp(2);
            VersionedMapNode m1 = registry.versionedNilMap.add("1", "a");
            db._setTimestamp(3);
            m1 = m1.remove("1", 0);

            VersionedMapNode m2 = m1.copyMap();
            m2.clearMap();
            assertEquals(0, m2.size(4));

            assertEquals(1, m1.totalSize("1"));

            assertEquals(0, m1.flatKeys(4).size());

            MapAccessor ma = m1.mapAccessor();
            assertNull(ma.firstKey());
            assertNull(ma.lastKey());
            assertEquals(0, ma.flatMap().size());

            assertNull(ma.higherKey(""));
            assertNull(ma.ceilingKey(""));
            assertNull(ma.ceilingKey("1"));
            assertNull(ma.lowerKey("9"));
            assertNull(ma.floorKey("9"));
            assertNull(ma.floorKey("1"));

            assertFalse(ma.iterator().hasNext());
        } finally {
            Plant.close();
        }
    }
}
