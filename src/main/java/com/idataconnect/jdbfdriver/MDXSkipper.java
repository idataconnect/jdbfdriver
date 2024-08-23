package com.idataconnect.jdbfdriver;

import java.io.IOException;
import java.util.Objects;

import com.idataconnect.jdbfdriver.index.MDX;

public class MDXSkipper implements Skipper {

    private final DBF dbf;
    private final MDX mdx;

    public MDXSkipper(DBF dbf, MDX mdx) {
        this.dbf = Objects.requireNonNull(dbf);
        this.mdx = Objects.requireNonNull(mdx);
    }

    @Override
    public int skip(int offset) throws IOException {
        int result = 0;
        if (offset == 0) {
            return this.dbf.recno();
        } else if (offset < 0) {
            for (int i = 0; i < offset; i++) {
                result = this.mdx.next();
                if (result <= 0) {
                    return result;
                }
            }
        } else {
            for (int i = 0; i > offset; i--) {
                result = this.mdx.prev();
                if (result <= 0) {
                    return result;
                }
            }
        }
        return result;
    }

}
