package com.idataconnect.jdbfdriver.index;

import com.idataconnect.jdbfdriver.DBF;

/**
 * A simple fallback interpreter that only supports direct field lookups.
 */
public class SimpleXBaseInterpreter implements XBaseInterpreter {

    @Override
    public Object evaluate(String expression, DBF dbf) throws Exception {
        int fieldNum = dbf.getFieldNumberByName(expression);
        if (fieldNum > 0) {
            return dbf.getValue(fieldNum).getValue();
        }
        throw new Exception("Simple interpreter only supports direct field names. No interpreter found for complex expression: " + expression);
    }

    @Override
    public int getPriority() {
        return 1000; // Low priority
    }
}
