/*
 * Copyright (c) 2009-2012, i Data Connect!
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of i Data Connect! nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.idataconnect.jdbfdriver.index;

import com.idataconnect.jdbfdriver.DBF;
import com.idataconnect.jdbfdriver.DBFDate;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * MDX multiple index implementation.
 */
public class MDX {

    private final ReentrantLock threadLock;
    private final File mdxFile;
    private final RandomAccessFile randomAccessFile;
    private final ByteBuffer buf;

    private String dbfName;
    private int blockSize;
    private int blockSizeMultiplier;
    private DBFDate reindexDate;
    private boolean production;
    private int entriesInTag;
    private int tagLength;
    private int tagsInUse;
    private int numberOfBlocks;
    private int firstFreeBlock;
    private int availableBlock;
    private DBFDate lastUpdateDate;
    private Tag[] tags;

    private MDX(File mdxFile, RandomAccessFile randomAccessFile, ReentrantLock threadLock) {
        this.mdxFile = mdxFile;
        this.randomAccessFile = randomAccessFile;
        this.threadLock = threadLock;
        this.buf = ByteBuffer.allocate(2048).order(ByteOrder.LITTLE_ENDIAN);
    }
    
    private class Tag {

        private int headerPage;
        private String name;
        private IndexDataType dataType;
        private int leftTag;
        private int rightTag;
        private int backwardTag;
        private int keyType;
        private boolean unique;
        private boolean descending;
        private int rootPage;
    }
    
    public static MDX open(File mdxFile) throws IOException {
        return open(mdxFile, new ReentrantLock());
    }
    
    public static MDX open(File mdxFile, ReentrantLock threadLock) throws IOException {
        final RandomAccessFile randomAccessFile = new RandomAccessFile(mdxFile, "rw" + (DBF.isSynchronousWritesEnabled() ? "s" : ""));
        final MDX mdx = new MDX(mdxFile, randomAccessFile, threadLock);
        mdx.readStructure();
        return mdx;
    }
    
    protected void readStructure() throws IOException {
        FileChannel channel = randomAccessFile.getChannel();
        
        // MDX header
        channel.position(0);
        buf.position(0);
        buf.limit(544);
        while (buf.hasRemaining()) {
            if (channel.read(buf) == -1) {
                throw new IOException("EOF while reading MDX structure");
            }
        }
        buf.position(0);

        byte version = buf.get();
        if (version != 2) {
            Logger.getLogger(getClass().getName()).log(Level.WARNING,
                    "MDX [%s] has unsupported version: %x", new Object[] {mdxFile, version});
        }
        int y = (buf.get() & 0xff) + 2000;
        int m = buf.get() & 0xff;
        int d = buf.get() & 0xff;
        reindexDate = new DBFDate(m, d, y);
        byte[] stringBytes = new byte[16];
        int i;
        byte b;
        for (i = 0; i < stringBytes.length; i++) {
            b = buf.get();
            if (b == 0) {
                break;
            }

            stringBytes[i] = b;
        }
        dbfName = new String(stringBytes, 0, i);
        buf.position(20);
        blockSizeMultiplier = buf.getShort() & 0xffff;
        blockSize = buf.getShort() & 0xffff;
        production = buf.get() != 0;
        entriesInTag = buf.get() & 0xff;
        if (entriesInTag > 48 || entriesInTag < 1) {
            throw new IOException("Invalid MDX header. Entries in tag=" + entriesInTag);
        }
        tagLength = buf.get() & 0xff;
        if (tagLength > 32 || tagLength < 1) {
            throw new IOException("Invalid MDX header. Tag length=" + tagLength);
        }
        buf.position(buf.position() + 1);
        tagsInUse = buf.getShort() & 0xffff;
        buf.position(buf.position() + 2);
        numberOfBlocks = (int) (buf.getInt() & 0xffffffffL);
        firstFreeBlock = (int) (buf.getInt() & 0xffffffffL);
        availableBlock = (int) (buf.getInt() & 0xffffffffL);
        y = (buf.get() & 0xff) + 2000;
        m = buf.get() & 0xff;
        d = buf.get() & 0xff;
        lastUpdateDate = new DBFDate(m, d, y);

        // Tags
        byte keyType, keyFormat;
        tags = new Tag[tagsInUse];
        for (int tagIndex = 0; tagIndex < tags.length; tagIndex++) {
            tags[tagIndex] = new Tag();

            channel.position(544 + tagIndex * tagLength);
            buf.position(0);
            buf.limit(21);
            while (buf.hasRemaining()) {
                if (channel.read(buf) == -1) {
                    throw new IOException("EOF while reading MDX [" + mdxFile.getName() + "] tag " + tagIndex);
                }
            }
            buf.position(0);
            tags[tagIndex].headerPage = (int) (buf.getInt() & 0xffffffffL);
            for (i = 0; i < 10; i++) {
                b = buf.get();
                if (b == 0) {
                    break;
                }

                stringBytes[i] = b;
            }
            tags[tagIndex].name = new String(stringBytes, 0, i);
            if (i < 10) {
                buf.position(buf.position() + 10 - i);
            }

            keyFormat = buf.get();
            tags[tagIndex].descending = (keyFormat & 0x08) == 0x08;
            tags[tagIndex].unique = (keyFormat & 0x40) == 0x40;
            tags[tagIndex].rightTag = buf.get() & 0xff;
            tags[tagIndex].leftTag = buf.get() & 0xff;
            tags[tagIndex].backwardTag = buf.get() & 0xff;
            buf.position(buf.position() + 1);
            keyType = buf.get();
            switch (keyType) {
                case 'C':
                    tags[tagIndex].dataType = IndexDataType.CHARACTER;
                    break;
                case 'N':
                    tags[tagIndex].dataType = IndexDataType.NUMERIC;
                    break;
                case 'D':
                    tags[tagIndex].dataType = IndexDataType.CHARACTER;
                    break;
                default:
                    throw new IOException("Unknown key type: " + (char) keyType);
            }

            // Tag header
            channel.position(tags[tagIndex].headerPage * 512);
            buf.position(0);
            buf.limit(512);
            while (buf.hasRemaining()) {
                if (channel.read(buf) == -1) {
                    throw new IOException("EOF while reading tag headers");
                }
            }
            buf.position(0);

            tags[tagIndex].rootPage = (int) (buf.getInt() & 0xffffffffL);
        }
    }

    /**
     * Prints the current MDX file's structure to <code>System.out</code>.
     */
    public void printStructure() {
        printStructure(System.out);
    }

    /**
     * Prints the current MDX file's structure to the requested print stream.
     * @param out The print stream to print the structure to.
     */
    public void printStructure(PrintStream out) {
        out.println("----------------------------------");
        
        out.printf("DBF Name:        %17s\n", dbfName);
        out.printf("Production:      %17b\n", production);
        out.printf("Block Size:      %17d\n", blockSize);
        out.printf("Block Size Mult: %17d\n", blockSizeMultiplier);
        out.printf("Entries In Tag:  %17d\n", entriesInTag);
        out.printf("Tag Length:      %17d\n", tagLength);
        out.printf("Tags In Use:     %17d\n", tagsInUse);
        out.printf("Number Of Blocks:%17d\n", numberOfBlocks);
        out.printf("First Free Block:%17d\n", firstFreeBlock);
        out.printf("Available Block: %17d\n", availableBlock);
        out.printf("Last Updated:    %17s\n", lastUpdateDate);
        out.printf("Reindex Date:    %17s\n", reindexDate);
        out.println("Tags:");
        
        for (int i = 0; i < tags.length; i++) {
            if (i != 0) {
                out.println(" ---");
            }

            out.printf(" Name:           %17s\n", tags[i].name);
            out.printf(" Header Page:    %17s\n", tags[i].headerPage);
            out.printf(" Root Page:      %17s\n", tags[i].rootPage);
        }

        out.println("----------------------------------");
    }
}
