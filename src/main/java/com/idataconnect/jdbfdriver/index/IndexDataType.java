package com.idataconnect.jdbfdriver.index;

/**
 * Represents the possible data types that xBase indexes can store. The ordinal
 * corresponds to the numeric value that is stored in the index file.
 */
public enum IndexDataType {

    /** Character (string) data type. */
    CHARACTER,
    /** Numeric data type. */
    NUMERIC,
    /** Date data type. */
    DATE,
    ;

    /**
     * Gets the index data type for the given value. This is the numeric value
     * that is stored in MDX and NDX indexes to represent the data type.
     *
     * @param value the numeric value representing the data type
     * @return the appropriate index data type, or <code>null</code> if no
     * type exists with the given value
     */
    public static IndexDataType valueOf(int value) {
        if (value >= 0 && value < values().length) {
            return values()[value];
        }

        throw new IllegalArgumentException("Invalid value " + value);
    }
}
