package com.idataconnect.jdbfdriver;

import static org.junit.jupiter.api.Assertions.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.idataconnect.jdbfdriver.index.MDX;
import com.idataconnect.jdbfdriver.index.IndexDataType;

public class DbfTestSuite {

    @TempDir
    File tempDir;

    private File dbfFile;
    private File mdxFile;

    @BeforeEach
    public void setup() throws IOException {
        dbfFile = new File(tempDir, "test.dbf");
        mdxFile = new File(tempDir, "test.mdx");
        
        List<DBFField> fields = new ArrayList<>();
        fields.add(new DBFField("ID", DBFField.FieldType.N, 10, 0));
        fields.add(new DBFField("NAME", DBFField.FieldType.C, 20, 0));
        DBF.create(dbfFile, fields);
    }

    @Test
    public void testDbfAndIndexLifecycle() throws Exception {
        DBF dbf = DBF.use(dbfFile);
        MDX mdx = MDX.create(mdxFile, "TEST");
        mdx.addTag("id_idx", "ID", IndexDataType.NUMERIC, false, false);
        mdx.setTag("id_idx");
        dbf.setIndex(mdx);

        // 1. Test basic insertion
        for (int i = 1; i <= 10; i++) {
            dbf.appendBlank();
            dbf.replace("ID", i);
            dbf.replace("NAME", "Record " + i);
        }
        
        // Verify forward traversal
        assertEquals(1, mdx.gotoTop());
        for (int i = 2; i <= 10; i++) {
            assertEquals(i, mdx.next());
        }
        assertEquals(DBF.RECORD_NUMBER_EOF, mdx.next());

        // Verify backward traversal
        assertEquals(10, mdx.gotoBottom());
        for (int i = 9; i >= 1; i--) {
            assertEquals(i, mdx.prev());
        }
        assertEquals(DBF.RECORD_NUMBER_BOF, mdx.prev());

        dbf.close();
        mdx.close();
    }

    @Test
    public void testIndexSplitting() throws Exception {
        DBF dbf = DBF.use(dbfFile);
        MDX mdx = MDX.create(mdxFile, "SPLIT");
        mdx.addTag("name_idx", "NAME", IndexDataType.CHARACTER, false, false);
        mdx.setTag("name_idx");
        dbf.setIndex(mdx);

        // Add 100 records to force multiple splits
        int count = 100;
        for (int i = 1; i <= count; i++) {
            dbf.appendBlank();
            // Pad i to ensure lexicographical order matches numeric order
            dbf.replace("NAME", String.format("%03d", i));
            dbf.replace("ID", i);
        }

        // Verify all records are present and in order
        int firstRec = mdx.gotoTop();
        // Since we inserted them in order, and name is %03d, record 1 should be first
        assertEquals(1, firstRec);
        
        for (int i = 2; i <= count; i++) {
            int nextRec = mdx.next();
            assertEquals(i, nextRec, "Record at index " + i + " is wrong");
        }
        assertEquals(DBF.RECORD_NUMBER_EOF, mdx.next());

        dbf.close();
        mdx.close();
    }
}
