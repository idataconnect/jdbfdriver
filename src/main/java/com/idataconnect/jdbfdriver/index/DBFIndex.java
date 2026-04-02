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

    /**
     * Inserts a key and record number into the index.
     *
     * @param key the key to insert
     * @param recordNumber the record number to associate with the key
     * @throws IOException if an I/O error occurs
     */
    void insert(Object key, int recordNumber) throws IOException;

    /**
     * Deletes a key and record number from the index.
     *
     * @param key the key to delete
     * @param recordNumber the record number to remove
     * @throws IOException if an I/O error occurs
     */
    void delete(Object key, int recordNumber) throws IOException;
}
