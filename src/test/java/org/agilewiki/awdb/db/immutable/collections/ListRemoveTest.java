package org.agilewiki.awdb.db.immutable.collections;

import junit.framework.TestCase;
import org.agilewiki.jactor2.core.impl.Plant;
import org.agilewiki.awdb.db.immutable.BaseRegistry;
import org.agilewiki.awdb.db.virtualcow.Db;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

public class ListRemoveTest extends TestCase {
    public void test() throws Exception {
        new Plant();
        try {
            Path dbPath = Paths.get("cow.db");
            int maxRootBlockSize = 1000;
            Db db = new Db(new BaseRegistry(), dbPath, maxRootBlockSize);
            DbFactoryRegistry registry = db.dbFactoryRegistry;

            ListNode l1 = registry.nilList;

            l1 = l1.remove(-1);
            l1 = l1.remove(0);
            l1 = l1.remove(1);

            ListNode l2 = registry.nilList.add("a");
            l2 = l2.add("b");
            l2 = l2.add("c");
            l2 = l2.add("d");
            l2 = l2.add("e");
            l2 = l2.add("f");
            l2 = l2.add("g");

            l2 = l2.remove(-3);
            l2 = l2.remove(-2);
            l2 = l2.remove(-1);
            assertEquals(7, l2.totalSize());
            assertEquals("abcdefg", String.join("", l2.flatList()));
            l2 = l2.remove(0);
            assertEquals(6, l2.totalSize());
            assertEquals("bcdefg", String.join("", l2.flatList()));
            l2 = l2.remove(0);
            assertEquals("cdefg", String.join("", l2.flatList()));
            l2 = l2.remove(0);
            assertEquals("defg", String.join("", l2.flatList()));
            l2 = l2.remove(0);
            assertEquals("efg", String.join("", l2.flatList()));
            ListNode copy = l2;
            l2 = l2.remove(0);
            assertEquals("fg", String.join("", l2.flatList()));
            l2 = l2.remove(0);
            assertEquals("g", String.join("", l2.flatList()));
            l2 = l2.remove(0);
            assertEquals("", String.join("", l2.flatList()));
            l2 = l2.remove(0);
            assertEquals("", String.join("", l2.flatList()));
            l2 = l2.remove(0);
            assertEquals("", String.join("", l2.flatList()));

            Iterator it = copy.iterator();
            assertTrue(it.hasNext());
            assertEquals("e", it.next());
            assertTrue(it.hasNext());
            assertEquals("f", it.next());
            assertTrue(it.hasNext());
            assertEquals("g", it.next());
            assertFalse(it.hasNext());
        } finally {
            Plant.close();
        }
    }
}
