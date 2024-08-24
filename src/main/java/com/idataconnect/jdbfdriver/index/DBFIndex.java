package com.idataconnect.jdbfdriver.index;

import java.io.IOException;

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
}
