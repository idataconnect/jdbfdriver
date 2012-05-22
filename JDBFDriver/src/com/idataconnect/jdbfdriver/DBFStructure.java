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

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Represents the structure of a DBF file. The most important attributes are
 * the fields in the DBF file, the header and record lengths, and when
 * the DBF file was last updated.
 * @author ben
 */
public class DBFStructure implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<DBFField> fields = new LinkedList<DBFField>();
    private boolean dbtPaired;
    private DBFDate lastUpdated;
    private int numberOfRecords;
    private short headerLength;
    private short recordLength;
    private boolean transactionActive;
    private boolean dataEncrypted;
    private boolean mdxPaired;
    private boolean memoExists;

    /**
     * Gets whether the DBF file is paired with a DBT file. This indicates
     * that a memo field is present.
     * @return whether the DBF file is DBT paired.
     */
    public boolean isDbtPaired() {
        return dbtPaired;
    }

    /**
     * Sets whether the DBF file is paired with a DBT file. This indicates
     * that a memo field is present.
     * @param dbtPaired whether a DBT file is paired with the DBF file.
     */
    public void setDbtPaired(boolean dbtPaired) {
        this.dbtPaired = dbtPaired;
    }

    /**
     * Gets the last updated date of the DBF file.
     * @return the last updated date of the DBF file.
     */
    public DBFDate getLastUpdated() {
        return lastUpdated;
    }

    /**
     * Sets the last updated date of the DBF file.
     * @param lastUpdated the last updated date of the DBF file.
     */
    public void setLastUpdated(DBFDate lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    /**
     * Gets the fields in the DBF file.
     * @return a list of fields.
     */
    public List<DBFField> getFields() {
        return fields;
    }

    /**
     * Sets the list of fields in the DBF file.
     * @param fields a list of fields.
     */
    public void setFields(List<DBFField> fields) {
        this.fields = fields;
    }

    /**
     * Gets the number of records in the DBF file.
     * @return the number of records in the DBF file.
     */
    public int getNumberOfRecords() {
        return numberOfRecords;
    }

    /**
     * Sets the number of records in the DBF file.
     * @param numberOfRecords the number of records in the DBF file.
     */
    public void setNumberOfRecords(int numberOfRecords) {
        this.numberOfRecords = numberOfRecords;
    }

    /**
     * Gets the length of the DBF file's header
     * @return the length of the DBF file's header.
     */
    public short getHeaderLength() {
        return headerLength;
    }

    /**
     * Sets the length of the DBF file's header.
     * @param headerLength
     */
    public void setHeaderLength(short headerLength) {
        this.headerLength = headerLength;
    }

    /**
     * Gets the length of each record in the DBF file.
     * @return the length of each record in the DBF file.
     */
    public short getRecordLength() {
        return recordLength;
    }

    /**
     * Sets the length of each record in the DBF file.
     * @param recordLength the length of each record in the DBF file.
     */
    public void setRecordLength(short recordLength) {
        this.recordLength = recordLength;
    }

    /**
     * Gets whether a transaction is currently active in the DBF file. This
     * is unsupported, and the value is simply what has been read
     * from the DBF file's header.
     * @return whether a transaction is active.
     */
    public boolean isTransactionActive() {
        return transactionActive;
    }

    /**
     * Sets whether a transaction is active. This is unsupported,
     * and the value is simply what has been read from the DBF file's header.
     * @param transactionActive
     */
    public void setTransactionActive(boolean transactionActive) {
        this.transactionActive = transactionActive;
    }

    /**
     * Gets whether the data is encrypted. This is unsupported and the value
     * is simply what has been read from the DBF file's header.
     * @return whether the data is encrypted.
     */
    public boolean isDataEncrypted() {
        return dataEncrypted;
    }

    /**
     * Sets whether the data is encrypted. This is unsupported and the
     * value is simply what has been read from the DBF file's header.
     * @param dataEncrypted
     */
    public void setDataEncrypted(boolean dataEncrypted) {
        this.dataEncrypted = dataEncrypted;
    }

    /**
     * Gets whether an MDX file is paired with this DBF file. Since MDX files
     * are currently unsupported, any modifications to DBF files with MDX files
     * will invalidate the contents of the MDX file.
     * @return whether an MDX file is paired with this DBF file.
     */
    public boolean isMdxPaired() {
        return mdxPaired;
    }

    /**
     * Sets whether an MDX file is paired with this DBF file. Since MDX files
     * are currently unsupported, any modifications to DBF files with MDX
     * files will invalidate the contents of the MDX file.
     * @param mdxPaired whether an MDX file is paired with this DBF file.
     */
    public void setMdxPaired(boolean mdxPaired) {
        this.mdxPaired = mdxPaired;
    }

    /**
     * Whether a memo field exists. This is presumably the same value as
     * <code>isDbtPaired()</code>.
     * @return whether a memo field exists.
     */
    public boolean isMemoExists() {
        return memoExists;
    }

    /**
     * Sets whether a memo field exists. This is presumably the same value
     * as <code>isDbtPaired</code>.
     * @param memoExists whether a memo field exists.
     */
    public void setMemoExists(boolean memoExists) {
        this.memoExists = memoExists;
    }
}
