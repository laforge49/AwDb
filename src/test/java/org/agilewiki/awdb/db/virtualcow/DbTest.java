package org.agilewiki.awdb.db.virtualcow;

import junit.framework.TestCase;
import org.agilewiki.jactor2.core.impl.Plant;
import org.agilewiki.awdb.db.ids.NameId;
import org.agilewiki.awdb.db.ids.composites.Journal;
import org.agilewiki.awdb.db.immutable.BaseRegistry;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;
import org.agilewiki.awdb.db.immutable.collections.ListAccessor;
import org.agilewiki.awdb.db.immutable.collections.MapAccessor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DbTest extends TestCase {
    public void test() throws Exception {
        new Plant();
        try {
            Path dbPath = Paths.get("vcow.db");
            Files.deleteIfExists(dbPath);
            int maxRootBlockSize = 1000;
            try (Db db = new Db(new BaseRegistry(), dbPath, maxRootBlockSize)) {
                Files.deleteIfExists(dbPath);
                db.registerTransaction("dbTran", DbTran.class);
                db.open(true);
                String timestampId = db.update("dbTran").call();
                db.close();

                System.out.println("db file size: " + Files.size(dbPath));

                System.out.println("\nAll keys:");
                MapAccessor dbMapAccessor = db.mapAccessor();
                for (ListAccessor la: dbMapAccessor) {
                    System.out.println(la.key());
                }

                System.out.println("\nJournal of x:");
                for (String tid: Journal.journal(db, NameId.generate("x"), FactoryRegistry.MAX_TIMESTAMP)) {
                    System.out.println(tid);
                }

                System.out.println("\nAll items modified by " + timestampId + ":");
                for (String id: Journal.modifies(db, timestampId, FactoryRegistry.MAX_TIMESTAMP)) {
                    System.out.println(id);
                }

                Display.all(db, db.getTimestamp());
            }
        } finally {
            Plant.close();
        }
    }
}
