package org.agilewiki.awdb.db.virtualcow;

import junit.framework.TestCase;
import org.agilewiki.jactor2.core.impl.Plant;
import org.agilewiki.awdb.db.immutable.BaseRegistry;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Link2Test extends TestCase {
    public void test() throws Exception {
        new Plant();
        try {
            Path dbPath = Paths.get("vcow.db");
            Files.deleteIfExists(dbPath);
            int maxRootBlockSize = 1000;
            try (Db db = new Db(new BaseRegistry(), dbPath, maxRootBlockSize)) {
                Files.deleteIfExists(dbPath);
                db.registerTransaction("Link2Tran", Link2Tran.class);
                db.open(true);
                String timestampId = db.update("Link2Tran").call();
            }
        } finally {
            Plant.close();
        }
    }
}
