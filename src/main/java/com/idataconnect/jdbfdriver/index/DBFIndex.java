package com.idataconnect.jdbfdriver.index;

import java.io.IOException;

/**
 * Contract for a type of DBF index.
 */
public interface DBFIndex {

    /**
     * Makes one step forward and returns the associated record number in the DBF file.
     * @return the record number in the DBF file
     * @throws IOException if an I/O error occurs
     */
    int next() throws IOException;

    /**
     * Makes one step backward and returns the associated record number in the DBF file.
     * @return the record number in the DBF file
     * @throws IOException if an I/O error occurs
     */
    int prev() throws IOException;

    /**
     * Goes to the first record for this index.
     *
     * @return the record number of the first record
     * @throws IOException if an I/O error occurs
     */
    public int gotoTop() throws IOException;

    /**
     * Goes to the last record for this index.
     *
     * @return the record number of the last record
     * @throws IOException if an I/O error occurs
     */
    public int gotoBottom() throws IOException;
}
