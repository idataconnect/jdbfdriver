package com.idataconnect.jdbfdriver;

import java.io.IOException;
import java.util.Objects;

/**
 * Implementation for a skipper that uses no index.
 */
public class NoIndexSkipper implements Skipper {

    private final DBF dbf;

    /**
     * Creates a new DBF skipper with the given DBF.
     */
    public NoIndexSkipper(DBF dbf) {
        this.dbf = Objects.requireNonNull(dbf);
    }

    @Override
    public int skip(int offset) throws IOException {
        return this.dbf.gotoRecord(this.dbf.recno() + offset);
    }

}
