/*
 * Copyright (c) 2009-2012, i Data Connect!
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of i Data Connect! nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.idataconnect.jdbfdriver;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * Represents any type of value stored in a DBF field.
 * @author ben
 */
public class DBFValue {

    private DBFField.FieldType fieldType;
    private Object value;

    /**
     * Constructs a new value with the given field and its object value.
     * @param field the DBF field which this is a value for
     * @param value the <code>Object</code> value which contains the data
     */
    public DBFValue(DBFField field, Object value) {
        this.fieldType = field.getFieldType();
        this.value = value;
    }

    /**
     * Sets the <code>Object</code> value for this DBF value, which contains
     * the data.
     * @param value the data value
     */
    public void setValue(Object value) {
        // Handle the case where they might set a date using a java.util.Date.
        if (value instanceof java.util.Date) {
            GregorianCalendar cal = new GregorianCalendar();
            cal.setTime((java.util.Date) value);
            this.value = new DBFDate(cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.YEAR));
        } else {
            this.value = value;
        }
    }

    /**
     * Gets the <code>Object</code> value for this DBF value.
     * @return the data value
     */
    public Object getValue() {
        return value;
    }

    /**
     * Gets the data as a <code>String</code>. This is equivalent to calling
     * <code>getValue().toString()</code>.
     * @return the value as a string
     */
    public String getString() {
        return value.toString();
    }

    /**
     * Gets the data as a <code>double</code>.
     * @throws IllegalStateException if the value is not a numeric type.
     * @return the value as a <code>double</code>
     */
    public double getDouble() {
        if (!isNumeric())
            throw new IllegalStateException("getDouble() called on a value which is not numeric");
        else
            return ((Number) value).doubleValue();
    }

    /**
     * Gets the data as an <code>int</code>.
     * @throws IllegalStateException if the value is not a numeric type.
     * @return the value as an <code>int</code>
     */
    public int getInt() {
        if (!isNumeric())
            throw new IllegalStateException("getInt() called on a value which is not numeric");
        else
            return ((Number) value).intValue();
    }

    /**
     * Gets the data as a <code>BigDecimal</code>.
     * @throws IllegalStateException if the value is not a numeric type.
     * @return the value as a <code>BigDecimal</code>
     */
    public BigDecimal getBigDecimal() {
        if (!isNumeric()) {
            throw new IllegalStateException("getBigDecimal() called on a value which is not numeric");
        } else {
            return (BigDecimal) value;
        }
    }

    /**
     * Gets the data as a <code>boolean</code>.
     * @throws IllegalStateException if the value is not a boolean type.
     * @return the value as a <code>boolean</code>
     */
    public boolean getBoolean() {
        if (!(value instanceof Boolean))
            throw new IllegalStateException("getBoolean() called on a value which is not a boolean");
        else
            return ((Boolean) value).booleanValue();
    }

    /**
     * Gets whether the value is a numeric type.
     * @return whether the value is a numeric type
     */
    public boolean isNumeric() {
        return fieldType.isNumericField();
    }

    /**
     * Gets whether the value is a character type.
     * @return whether the value is a character type
     */
    public boolean isString() {
        return fieldType.isCharacterField();
    }

    /**
     * Gets the data as a <code>DBFDate</code>.
     * @throws IllegalStateException if the value is not a date type
     * @return the value as a DBF date
     */
    public DBFDate getDate() {
        if (!(value instanceof DBFDate))
            throw new IllegalStateException("getDate() called on a value which is not a date");
        else
            return (DBFDate) value;
    }

    /**
     * Gets the data as bytes.
     * @return the value as as byte array
     */
    public byte[] getBytes() {
        switch (getFieldType()) {
            case B:
            case G:
                return (byte[]) value;
            default:
                return value.toString().getBytes();
        }
    }

    /**
     * Gets the field type of this value.
     * @return the field type of this value
     */
    public DBFField.FieldType getFieldType() {
        return fieldType;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation returns a value suitable for display to the user.
     * </p>
     */
    @Override
    public String toString() {
        return "[" + fieldType.getFullName() + ": " + value.toString() + "]";
    }
}
