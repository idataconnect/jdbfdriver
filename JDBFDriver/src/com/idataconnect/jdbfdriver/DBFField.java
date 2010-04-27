/*
 * Copyright (c) 2009-2010, i Data Connect!
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

/**
 * Represents a field in a DBF file.
 * @author ben
 */
public class DBFField {

    /** The type of the DBF field, describing the data it represents. */
    public enum FieldType {

        /** Character field type */
        C("Character", false, true, false, false, false),
        /** Numeric field type */
        N("Numeric", false, false, false, true, false),
        /** Logical (boolean) field type */
        L("Logical", false, false, false, false, true),
        /** Date field type */
        D("Date", false, false, true, false, false),
        /** Memo (textual blob) field type */
        M("Memo", true, true, false, false, false),
        /** Binary (blob) field type */
        B("Binary", true, false, false, false, false),
        /** General field type */
        G("General", true, false, false, false, false),
        /** Float field type */
        F("Float", false, false, false, true, false),
        /** Unknown field type */
        U("Unknown", false, false, false, false, false);

        private String fullName;
        private boolean dbtField;
        private boolean characterField;
        private boolean dateField;
        private boolean numericField;
        private boolean booleanfield;

        FieldType(String fullName, boolean dbtField, boolean characterField,
                boolean dateField, boolean numericField, boolean booleanField) {
            this.fullName = fullName;
            this.dbtField = dbtField;
            this.characterField = characterField;
            this.dateField = dateField;
            this.numericField = numericField;
            this.booleanfield = booleanField;
        }

        /**
         * Gets the full (display) name for this field.
         * @return the full name string.
         */
        public String getFullName() {
            return fullName;
        }

        /**
         * Gets whether this field is a boolean field. Currently, the only
         * field that is a boolean field is the <b>L</b> field type.
         * @return whether the field is a boolean field.
         */
        public boolean isBooleanfield() {
            return booleanfield;
        }

        /**
         * Gets whether this field is a character field.
         * @return whether this field is a character field.
         */
        public boolean isCharacterField() {
            return characterField;
        }

        /**
         * Gets whether this field is a date field.
         * @return whether this field is a date field.
         */
        public boolean isDateField() {
            return dateField;
        }

        /**
         * Gets whether this field is a DBT (memo) field.
         * @return whether this field is a DBT field.
         */
        public boolean isDbtField() {
            return dbtField;
        }

        /**
         * Gets whether this field is a numeric field.
         * @return whether this field is a numeric field.
         */
        public boolean isNumericField() {
            return numericField;
        }
    }
    private String fieldName = "UNKNOWN";
    private FieldType fieldType = FieldType.U;
    private int fieldLength;
    private int decimalLength;

    /**
     * Creates a new, empty field, initially set to an unknown field type.
     */
    public DBFField() {
    }

    /**
     * Creates a field with the specified attributes.
     * @param fieldName The name of the field.
     * @param fieldType The type of the field.
     * @param fieldLength The length of the field.
     * @param decimalLength The length of the decimal portion of the field.
     */
    public DBFField(String fieldName, FieldType fieldType, int fieldLength, int decimalLength) {
        this.fieldName = fieldName.toUpperCase();
        this.fieldType = fieldType;
        this.fieldLength = fieldLength;
        this.decimalLength = decimalLength;

        // Force values for certain field types:
        // Date fields have a length of 8 and a decimal length of 0
        // Character fields have a decimal length of 0
        // Logical fields have a length of 1 and a decimal length of 0
        switch (fieldType) {
            case L:
                this.fieldLength = 1;
                this.decimalLength = 0;
                break;

            case D:
                this.fieldLength = 8;
            case C:
            case M:
                this.decimalLength = 0;
        }
    }

    /**
     * Creates a field and specifies all of the field's attributes,
     * using a string for the field type.
     * @param fieldName The name of the field.
     * @param fieldTypeCode The code for the field type (e.g. "C")
     * @param fieldLength The length of the field.
     * @param decimalLength The length of the decimal portion of the field.
     */
    public DBFField(String fieldName, String fieldTypeCode, int fieldLength, int decimalLength) {
        this(fieldName, FieldType.valueOf(fieldTypeCode), fieldLength, decimalLength);
    }

    /**
     * Gets the default value for the current field type.
     * @return The default value.
     */
    public DBFValue getDefaultValue() {
        switch (getFieldType()) {
            case C:
            case M:
            default:
                return new DBFValue(this, "");
            case B:
            case G:
                return new DBFValue(this, new byte[0]);
            case N:
            case F:
                return new DBFValue(this, new Double(0));
            case L:
                return new DBFValue(this, Boolean.FALSE);
            case D:
                return new DBFValue(this, new DBFDate(0, 0, 0));
        }
    }

    /**
     * Gets the name of the field.
     * @return The name of the field.
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * Sets the name of the field.
     * @param fieldName The name of the field.
     */
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    /**
     * Gets the length of the decimal portion of the field.
     * @return The length of the decimal portion of the field.
     */
    public int getDecimalLength() {
        return decimalLength;
    }

    /**
     * Sets the length of the decimal portion of the field.
     * @param decimalLength The length of the decimal portion of the field.
     */
    public void setDecimalLength(int decimalLength) {
        this.decimalLength = decimalLength;
    }

    /**
     * Gets the length of the field.
     * @return The length of the field.
     */
    public int getFieldLength() {
        return fieldLength;
    }

    /**
     * Sets the length of the field.
     * @param fieldLength The length of the field.
     */
    public void setFieldLength(int fieldLength) {
        this.fieldLength = fieldLength;
    }

    /**
     * Gets the type of the field.
     * @return The type of the field.
     */
    public FieldType getFieldType() {
        return fieldType;
    }

    /**
     * Sets the type of the field.
     * @param fieldType The type of the field.
     */
    public void setFieldType(FieldType fieldType) {
        this.fieldType = fieldType;
    }
}
