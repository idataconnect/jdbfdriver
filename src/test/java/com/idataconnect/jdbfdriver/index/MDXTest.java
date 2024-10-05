package com.idataconnect.jdbfdriver.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.idataconnect.jdbfdriver.DBF;
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
        Optional<Tag> ot = mdx.setTag("test1");
        assertTrue(ot.isPresent());
        assertEquals(2, mdx.find("test2"));
        assertEquals(DBF.RECORD_NUMBER_EOF, mdx.find("nonexistent"));
    }

    @Test
    public void testFindIntegerInFloatTag() throws Exception {
        MDX mdx = MDX.open(createMdxInstance("TEST.MDX"));
        Optional<Tag> ot = mdx.setTag("test2");
        assertTrue(ot.isPresent());
        assertEquals(1, mdx.find(10));
        assertEquals(DBF.RECORD_NUMBER_EOF, mdx.find(30));
        assertEquals(3, mdx.find(15));
        assertEquals(2, mdx.find(20));
    }

    @Test
    public void testDecodeIntegers() throws Exception {
        byte[] buf = { 0x36, 0x29, 0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        ByteBuffer bb = ByteBuffer.wrap(buf);
        double result = MDX.decodeNumeric(bb);
        assertEquals(10d, result);

        buf = new byte[] { 0x36, 0x29, 0x15, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        bb = ByteBuffer.wrap(buf);
        result = MDX.decodeNumeric(bb);
        assertEquals(15d, result);

        buf = new byte[] { 0x36, 0x29, 0x20, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        bb = ByteBuffer.wrap(buf);
        result = MDX.decodeNumeric(bb);
        assertEquals(20d, result);

        buf = new byte[] { 0x3a, 0x51, 0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        bb = ByteBuffer.wrap(buf);
        result = MDX.decodeNumeric(bb);
        assertEquals(100_000d, result);

        buf = new byte[] { 0x3d, 0x51, (byte) 0x99, (byte) 0x99, (byte) 0x99, (byte) 0x99, (byte) 0x90, 0, 0, 0, 0, 0 };
        bb = ByteBuffer.wrap(buf);
        result = MDX.decodeNumeric(bb);
        assertEquals(999_999_999d, result);

        buf = new byte[] { 0x3e, 0x51, (byte) 0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        bb = ByteBuffer.wrap(buf);
        result = MDX.decodeNumeric(bb);
        assertEquals(1_000_000_000d, result);
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
