package org.agilewiki.awdb.db.immutable.collections;

import junit.framework.TestCase;
import org.agilewiki.jactor2.core.impl.Plant;
import org.agilewiki.awdb.db.immutable.BaseRegistry;
import org.agilewiki.awdb.db.virtualcow.Db;
import org.agilewiki.awdb.db.virtualcow.DbFactoryRegistry;

import java.nio.file.Path;
import java.nio.file.Paths;

public class MapRemoveTest extends TestCase {
    public void test() throws Exception {
        new Plant();
        try {
            Path dbPath = Paths.get("cow.db");
            int maxRootBlockSize = 1000;
            Db db = new Db(new BaseRegistry(), dbPath, maxRootBlockSize);
            DbFactoryRegistry registry = db.dbFactoryRegistry;

            assertEquals(registry.nilMap, registry.nilMap.remove("", 0));
            assertEquals(1, registry.nilMap.set("1", "a").size());

            MapNode m1 = registry.nilMap.add("1", "a");
            m1 = m1.add("2", "a");
            m1 = m1.add("3", "a");
            m1 = m1.add("4", "a");
            m1 = m1.add("5", "a");
            m1 = m1.add("6", "a");
            m1 = m1.add("7", "a");

            assertEquals("1234567", String.join("", m1.flatKeys()));
            m1 = m1.remove("7", 0);
            assertEquals(0, m1.totalSize("7"));
            assertEquals("123456", String.join("", m1.flatKeys()));
            m1 = m1.remove("6");
            assertEquals("12345", String.join("", m1.flatKeys()));
            m1 = m1.remove("5");
            assertEquals("1234", String.join("", m1.flatKeys()));
            m1 = m1.remove("4");
            assertEquals("123", String.join("", m1.flatKeys()));
            m1 = m1.remove("3");
            assertEquals("12", String.join("", m1.flatKeys()));
            m1 = m1.remove("2");
            assertEquals("1", String.join("", m1.flatKeys()));
            m1 = m1.remove("1");
            assertEquals("", String.join("", m1.flatKeys()));
        } finally {
            Plant.close();
        }
    }
}
