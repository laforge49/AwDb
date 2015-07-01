package org.agilewiki.awdb.db.ids;

/**
 * Usable only as a postfix to a composite id, there are no restrictions on the
 * content of a ValueId.
 */
public class ValueId {
    /**
     * Used to add an arbitrary string to a composite id.
     */
    public static final String PREFIX = "$v";

    /**
     * Generate an id by prefixing a string value with $v.
     *
     * @param value    A string.
     * @return The string $v + value.
     */
    public static String generate(String value) {
        return PREFIX + value;
    }

    /**
     * Validate a valueId.
     *
     * @param valueId    The valueId.
     */
    public static void validateAnId(String valueId) {
        if (!valueId.startsWith("$"))
            throw new IllegalArgumentException("not a valid Id: "+valueId);
    }

    /**
     * Validate a valueId.
     *
     * @param valueId    The valueId.
     */
    public static void validateId(String valueId) {
        if (!valueId.startsWith("$v"))
            throw new IllegalArgumentException("not a valid valueId: "+valueId);
    }

    /**
     * Returns the value of a valueId.
     *
     * @param valueId    The valueId.
     * @return The value.
     */
    public static String value(String valueId) {
        validateId(valueId);
        return valueId.substring(2);
    }
}
