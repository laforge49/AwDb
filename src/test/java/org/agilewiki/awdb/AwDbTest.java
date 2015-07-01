package org.agilewiki.awdb;

import junit.framework.TestCase;
import org.agilewiki.awdb.db.immutable.FactoryRegistry;
import org.agilewiki.awdb.db.virtualcow.DbTran;
import org.agilewiki.jactor2.core.impl.Plant;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class AwDbTest extends TestCase {
    public void test() throws Exception {
        new Plant();
        try {
            Path dbPath = Paths.get("vcow.db");
            Files.deleteIfExists(dbPath);
            int maxRootBlockSize = 100000;
            long maxNodeCacheSize = 10000;
            Path journalDirectoryPath = Paths.get("journals");
            boolean clearJournals = true;
            try (AwDb awDb = new AwDb(dbPath, maxRootBlockSize, maxNodeCacheSize,
                    journalDirectoryPath, clearJournals)) {
                awDb.registerTransaction(AwDbTran.NAME, AwDbTran.class);
                awDb.update(AwDbTran.NAME, awDb.nilMap).call();
                awDb.displayAll(FactoryRegistry.MAX_TIMESTAMP);
                awDb.close();
            }
        } finally {
            Plant.close();
        }
    }
}
