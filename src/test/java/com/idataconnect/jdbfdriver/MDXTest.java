package com.idataconnect.jdbfdriver;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.jupiter.api.Test;

import com.idataconnect.jdbfdriver.index.MDX;

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
}
