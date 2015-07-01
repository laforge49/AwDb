package org.agilewiki.awdb.db.immutable.collections;

import junit.framework.TestCase;
import org.agilewiki.jactor2.core.impl.Plant;
import org.agilewiki.awdb.db.immutable.BaseRegistry;
import org.agilewiki.awdb.db.virtualcow.Db;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.file.Path;
import java.nio.file.Paths;

public class ListInsertTest extends TestCase {
    public void test() throws Exception {
        new Plant();
        try {
            Path dbPath = Paths.get("cow.db");
            int maxRootBlockSize = 1000;
            Db db = new Db(new BaseRegistry(), dbPath, maxRootBlockSize);
            DbFactoryRegistry registry = db.dbFactoryRegistry;

            ListNode l1 = registry.nilList.add("a");
            assertEquals(1, l1.totalSize());
            l1 = l1.add("b");
            assertEquals(2, l1.totalSize());
            l1 = l1.add("c");
            assertEquals(3, l1.totalSize());
            l1 = l1.add("d");
            l1 = l1.add("e");
            l1 = l1.add("f");
            l1 = l1.add("g");
            assertEquals(7, l1.totalSize());
            ListAccessor a1 = l1.listAccessor();
            assertEquals("a", a1.get(0));
            assertEquals("b", a1.get(1));
            assertEquals("c", a1.get(2));
            assertEquals("d", a1.get(3));
            assertEquals("e", a1.get(4));
            String f = "f";
            assertEquals(f, a1.get(5));
            assertEquals("g", a1.get(6));
            assertEquals("abcdefg", String.join("", a1.flatList()));

            ListNode l2 = registry.nilList.add(0, "G");
            l2 = l2.add(0, "F");
            l2 = l2.add(0, "E");
            l2 = l2.add(0, "D");
            l2 = l2.add(0, "C");
            l2 = l2.add(0, "B");
            l2 = l2.add(0, "A");
            assertEquals(7, l2.totalSize());
            ListAccessor a2 = l2.listAccessor();
            assertEquals("A", a2.get(0));
            assertEquals("B", a2.get(1));
            assertEquals("C", a2.get(2));
            assertEquals("D", a2.get(3));
            assertEquals("E", a2.get(4));
            assertEquals("F", a2.get(5));
            assertEquals("G", a2.get(6));
            assertEquals(7, a2.size());
            assertEquals("ABCDEFG", String.join("", a2.flatList()));

            assertFalse(a1.isEmpty());

            assertEquals(0, a1.higherIndex(-3));
            assertEquals(0, a1.higherIndex(-2));
            assertEquals(0, a1.higherIndex(-1));
            assertEquals(1, a1.higherIndex(0));
            assertEquals(2, a1.higherIndex(1));
            assertEquals(3, a1.higherIndex(2));
            assertEquals(4, a1.higherIndex(3));
            assertEquals(5, a1.higherIndex(4));
            assertEquals(6, a1.higherIndex(5));
            assertEquals(-1, a1.higherIndex(6));
            assertEquals(-1, a1.higherIndex(7));
            assertEquals(-1, a1.higherIndex(8));

            assertEquals(0, a1.ceilingIndex(-3));
            assertEquals(0, a1.ceilingIndex(-2));
            assertEquals(0, a1.ceilingIndex(-1));
            assertEquals(0, a1.ceilingIndex(0));
            assertEquals(1, a1.ceilingIndex(1));
            assertEquals(2, a1.ceilingIndex(2));
            assertEquals(3, a1.ceilingIndex(3));
            assertEquals(4, a1.ceilingIndex(4));
            assertEquals(5, a1.ceilingIndex(5));
            assertEquals(6, a1.ceilingIndex(6));
            assertEquals(-1, a1.ceilingIndex(7));
            assertEquals(-1, a1.ceilingIndex(8));

            assertEquals(-1, a1.lowerIndex(-3));
            assertEquals(-1, a1.lowerIndex(-2));
            assertEquals(-1, a1.lowerIndex(-1));
            assertEquals(-1, a1.lowerIndex(0));
            assertEquals(0, a1.lowerIndex(1));
            assertEquals(1, a1.lowerIndex(2));
            assertEquals(2, a1.lowerIndex(3));
            assertEquals(3, a1.lowerIndex(4));
            assertEquals(4, a1.lowerIndex(5));
            assertEquals(5, a1.lowerIndex(6));
            assertEquals(6, a1.lowerIndex(7));
            assertEquals(6, a1.lowerIndex(8));

            assertEquals(-1, a1.floorIndex(-3));
            assertEquals(-1, a1.floorIndex(-2));
            assertEquals(-1, a1.floorIndex(-1));
            assertEquals(0, a1.floorIndex(0));
            assertEquals(1, a1.floorIndex(1));
            assertEquals(2, a1.floorIndex(2));
            assertEquals(3, a1.floorIndex(3));
            assertEquals(4, a1.floorIndex(4));
            assertEquals(5, a1.floorIndex(5));
            assertEquals(6, a1.floorIndex(6));
            assertEquals(6, a1.floorIndex(7));
            assertEquals(6, a1.floorIndex(8));

            assertEquals(0, a1.firstIndex());
            assertEquals(6, a1.lastIndex());

            assertEquals(5, a1.getIndex(f));
            assertEquals(5, a1.getIndexRight(f));
            assertEquals(5, a1.findIndex("f"));
            assertEquals(5, a1.findIndexRight("f"));
        } finally {
            Plant.close();
        }
    }
}
