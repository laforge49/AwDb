package org.agilewiki.awdb.db.ids;

import org.agilewiki.awdb.nodes.User_Node;

import java.math.BigInteger;
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
        return PREFIX + generate64() + generate64();
    }

    public synchronized String generateSeed() {
        return User_Node.bytesToHex(secureRandom.generateSeed(16));
    }

    public static SecureRandom newSecureRandom(String seed) {
        return new SecureRandom(new BigInteger(seed, 16).toByteArray());
    }

    private static String generate64(SecureRandom secureRandom) {
        return Long.toHexString(secureRandom.nextLong());
    }

    public static String generate(SecureRandom secureRandom) {
        return PREFIX + generate64(secureRandom) + generate64(secureRandom);
    }
}
