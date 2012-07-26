package com.idataconnect.jdbfdriver.index;

/**
 * Data type which xBase indexes can store.
 */
public enum IndexDataType {

    /** Character (string) data type. */
    CHARACTER(0),
    /** Numeric data type. */
    NUMERIC(1),
    /** Date data type. */
    DATE(2),
    ;

    private final int value;

    private IndexDataType(int value) {
        this.value = value;
    }

    /**
     * Gets the index data type with the given value. This is the numeric value
     * which is stored in MDX and NDX indexes to represent the data type.
     *
     * @param value the numeric value representing the data type
     * @return the appropriate index data type, or <code>null</code> if no
     * type exists with the given value
     */
    public static IndexDataType valueOf(int value) {
        for (IndexDataType idt : values()) {
            if (idt.value == value) {
                return idt;
            }
        }

        return null;
    }
}
