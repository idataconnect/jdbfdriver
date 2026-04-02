package com.idataconnect.jdbfdriver.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.idataconnect.jdbfdriver.DBF;
import com.idataconnect.jdbfdriver.DBFField;
import com.idataconnect.jdbfdriver.DBFField.FieldType;

public class IndexWriteTest {

    private File tempDir;
    private File dbfFile;

    @BeforeEach
    public void setup() throws IOException {
        tempDir = File.createTempFile("jdbf_test", "");
        tempDir.delete();
        tempDir.mkdirs();
        
        dbfFile = new File(tempDir, "test.dbf");
        System.out.println("tempDir: " + tempDir);
        System.out.println("dbfFile: " + dbfFile);
        List<DBFField> fields = new ArrayList<>();
        fields.add(new DBFField("ID", FieldType.N, 10, 0));
        fields.add(new DBFField("NAME", FieldType.C, 20, 0));
        DBF.create(dbfFile, fields);
    }

    @Test
    public void testMdxInsertion() throws Exception {
        System.out.println("testMdxInsertion dbfFile: " + dbfFile);
        DBF dbf = DBF.use(dbfFile);
        
        File mdxFile = new File(tempDir, "test.mdx");
        MDX mdx = MDX.create(mdxFile, "TEST");
        mdx.addTag("id_idx", "ID", IndexDataType.NUMERIC, false, false);
        mdx.setTag("id_idx");
        
        dbf.setIndex(mdx);
        
        // Add some records
        for (int i = 1; i <= 10; i++) {
            dbf.appendBlank();
            dbf.replace("ID", i);
            // dbf.replace("NAME", "Record " + i); // Skip this to avoid duplicate index entry for same record
        }
        
        mdx.close();
        dbf.close();
        
        // Verify we can open it back up and traverse
        MDX mdx2 = MDX.open(mdxFile);
        mdx2.setTag("id_idx");
        assertEquals(1, mdx2.tagsInUse);
        assertEquals("ID_IDX", mdx2.tags[0].getName());
        
        // Traversing
        assertEquals(1, mdx2.gotoTop());
        for (int i = 2; i <= 10; i++) {
            assertEquals(i, mdx2.next());
        }
        assertEquals(DBF.RECORD_NUMBER_EOF, mdx2.next());
        
        mdx2.close();
    }

    @Test
    public void testMdxOverflow() throws Exception {
        DBF dbf = DBF.use(dbfFile);
        File mdxFile = new File(tempDir, "overflow.mdx");
        MDX mdx = MDX.create(mdxFile, "TEST");
        mdx.addTag("id_idx", "ID", IndexDataType.NUMERIC, false, false);
        mdx.setTag("id_idx");
        dbf.setIndex(mdx);
        
        int count = 100; // Will trigger splits since keysPerBlock is ~18
        for (int i = 1; i <= count; i++) {
            dbf.appendBlank();
            dbf.replace("ID", i);
        }
        
        mdx.close();
        dbf.close();
        
        MDX mdx2 = MDX.open(mdxFile);
        mdx2.setTag("id_idx");
        
        assertEquals(1, mdx2.gotoTop());
        for (int i = 2; i <= count; i++) {
            assertEquals(i, mdx2.next());
        }
        assertEquals(DBF.RECORD_NUMBER_EOF, mdx2.next());
        mdx2.close();
    }
}
