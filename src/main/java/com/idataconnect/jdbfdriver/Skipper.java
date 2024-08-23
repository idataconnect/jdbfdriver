package com.idataconnect.jdbfdriver;

import java.io.IOException;

public interface Skipper {

    /**
     * Skips forwards or backwards, based on the given offset. When the skip is successful,
     * the new record number will be returned. If there are no more records to encounter,
     * either {@link DBF.RECORD_NUMBER_EOF} or {@link DBF.RECORD_NUMBER_BOF} will be returned,
     * depending on the skip direction.
     *
     * @param offset the number of records to skip - may be negative for reverse skipping
     * @return the record number, or one of the special markers
     * @throws IOException if an I/O error occurs
     */
    int skip(int offset) throws IOException;
}
