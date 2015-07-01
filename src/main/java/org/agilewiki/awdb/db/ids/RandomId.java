package org.agilewiki.awdb.db.ids;

import java.security.SecureRandom;

/**
 * Creates a random hex string, prefaced with $r.
 */
public class RandomId {
    public static final String PREFIX = "$r";
    private SecureRandom secureRandom = new SecureRandom();

    private String generate64() {
        return Long.toHexString(secureRandom.nextLong());
    }

    /**
     * Returns a 32 digit random hex string.
     *
     * @return A secure random identifier that starts with $r.
     */
    public synchronized String generate() {
        return PREFIX+generate64() + generate64();
    }
}
