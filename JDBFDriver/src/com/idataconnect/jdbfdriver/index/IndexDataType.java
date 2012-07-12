package com.idataconnect.jdbfdriver.index;

/**
 *
 */
public enum IndexDataType {

    CHARACTER(0),
    NUMERIC(1),
    DATE(2),
    ;

    private final int value;

    private IndexDataType(int value) {
        this.value = value;
    }

    public static IndexDataType valueOf(int value) {
        switch (value) {
            case 0:
            default:
                return CHARACTER;
            case 1:
                return NUMERIC;
            case 2:
                return DATE;
        }
    }
}
