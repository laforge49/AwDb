package org.agilewiki.awdb.db.immutable.collections;

import junit.framework.TestCase;
import org.agilewiki.jactor2.core.impl.Plant;
import org.agilewiki.awdb.db.immutable.BaseRegistry;
import org.agilewiki.awdb.db.virtualcow.Db;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.file.Path;
import java.nio.file.Paths;

public class ListEmptyTest extends TestCase {
    public void test() throws Exception {
        new Plant();
        try {
            Path dbPath = Paths.get("cow.db");
            int maxRootBlockSize = 1000;
            Db db = new Db(new BaseRegistry(), dbPath, maxRootBlockSize);
            DbFactoryRegistry registry = db.dbFactoryRegistry;

            assertEquals(0, registry.nilList.totalSize());

            ListAccessor la = registry.nilList.listAccessor();

            assertEquals(BaseRegistry.MAX_TIMESTAMP, la.getTimestamp());

            assertNull(la.get(-1));
            assertNull(la.get(0));
            assertNull(la.get(1));

            assertEquals(-1, la.higherIndex(-1));
            assertEquals(-1, la.higherIndex(0));
            assertEquals(-1, la.higherIndex(1));

            assertEquals(-1, la.ceilingIndex(-1));
            assertEquals(-1, la.ceilingIndex(0));
            assertEquals(-1, la.ceilingIndex(1));

            assertEquals(-1, la.lowerIndex(-1));
            assertEquals(-1, la.lowerIndex(0));
            assertEquals(-1, la.lowerIndex(1));

            assertEquals(-1, la.floorIndex(-1));
            assertEquals(-1, la.floorIndex(0));
            assertEquals(-1, la.floorIndex(1));

            assertEquals(-1, la.firstIndex());

            assertEquals(-1, la.lastIndex());

            assertTrue(la.isEmpty());

            assertEquals(0, la.flatList().size());

            assertEquals(-1, la.getIndex(""));
            assertEquals(-1, la.getIndexRight(""));
            assertEquals(-1, la.findIndex(""));
            assertEquals(-1, la.findIndexRight(""));

            assertFalse(la.iterator().hasNext());

            assertEquals(0, la.size());
        } finally {
            Plant.close();
        }
    }
}
