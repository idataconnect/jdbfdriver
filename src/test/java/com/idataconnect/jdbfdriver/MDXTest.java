package com.idataconnect.jdbfdriver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.idataconnect.jdbfdriver.index.MDX;
import com.idataconnect.jdbfdriver.index.MDX.Tag;

public class MDXTest {

    private File createMdxInstance(String filename) throws IOException {
        try (InputStream is = getClass().getResourceAsStream(filename)) {
            if (is == null) {
                fail("Unable to load resource " + filename);
            }

            File file = File.createTempFile("test_mdx", ".mdx");
            file.deleteOnExit();
            try (FileOutputStream os = new FileOutputStream(file)) {
                int b;
                while ((b = is.read()) != -1) {
                    os.write(b);
                }
            }
            return file;
        }
    }

    @Test
    public void testReadStructure() throws Exception {
        MDX mdx = MDX.open(createMdxInstance("TEST.MDX"));
        mdx.printStructure();
    }

    @Test
    public void testFindString() throws Exception {
        MDX mdx = MDX.open(createMdxInstance("TEST.MDX"));
    }

    @Test
    public void testWalkIndexForward() throws Exception {
        MDX mdx = MDX.open(createMdxInstance("TEST.MDX"));
        Optional<Tag> ot = mdx.setTag("test1");
        assertTrue(ot.isPresent());
        assertEquals(3, mdx.gotoTop());
        assertEquals(1, mdx.next());
        assertEquals(2, mdx.next());
        assertEquals(DBF.RECORD_NUMBER_EOF, mdx.next());
    }

    @Test
    public void testGotoBottom() throws Exception {
        MDX mdx = MDX.open(createMdxInstance("TEST.MDX"));
        Optional<Tag> ot = mdx.setTag("test1");
        assertTrue(ot.isPresent());
        assertEquals(2, mdx.gotoBottom());
    }

    @Test
    public void testWalkIndexBackward() throws Exception {
        MDX mdx = MDX.open(createMdxInstance("TEST.MDX"));
        Optional<Tag> ot = mdx.setTag("test1");
        assertTrue(ot.isPresent());
        assertEquals(2, mdx.gotoBottom());
        assertEquals(1, mdx.prev());
        assertEquals(3, mdx.prev());
        assertEquals(DBF.RECORD_NUMBER_BOF, mdx.prev());
    }
}
